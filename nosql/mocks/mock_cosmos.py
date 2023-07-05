import functools
import json
import re
from urllib import parse as urlparse

import boto3  # type: ignore
import responses  # type: ignore

from nosql.mocks import query_table
from nosql.table import deserialize


def mock_cosmos(f):

    def _get_key(headers, _table, doc_id):
        hk = None
        rk = None
        for kd in _table.key_schema:
            if kd['KeyType'] == 'HASH':
                hk = kd['AttributeName']
            else:
                rk = kd['AttributeName']
        _part_keys = headers.get('x-ms-documentdb-partitionkey')
        if isinstance(_part_keys, str) and len(_part_keys) > 0:
            _part_val = json.loads(_part_keys)[0]
        else:
            _part_val = doc_id
        key = {
            hk: _part_val
        }
        if rk is not None:
            key[rk] = doc_id
        return key

    def _callback(request):
        path = urlparse.urlsplit(request.url).path
        headers = dict(request.headers)

        def _response(code=404, body=None, _headers=None):
            return (
                code, _headers or {}, json.dumps({
                    "Errors": [
                        "Resource Not Found. "
                        "Learn more: https://aka.ms/cosmosdb-tsg-not-found"
                    ]
                }) if code == 404
                else json.dumps(body) if body is not None
                else body
            )

        parts = [_ for _ in path.split('/') if _ != '']
        # print(f'REQ: {request.method} {path} H: {headers} B: {request.body}')

        # required for CosmosClient
        if request.method == 'GET' and path == '/':
            return _response(
                200, {
                    "userConsistencyPolicy": {
                        "defaultConsistencyLevel": "Session"
                    }
                }
            )

        if len(parts) < 4 or parts[0] != 'dbs':
            return _response(404)

        table_name = parts[3]

        # use moto dynamodb to mock cosmos :-/
        _table = boto3.resource('dynamodb').Table(table_name)

        # /dbs/{database}/colls/{table}/docs/{docid}
        if len(parts) == 6 and parts[-2] == 'docs':
            key = _get_key(headers, _table, parts[-1])
            if request.method == 'GET':
                response = _table.get_item(Key=key)
                _item = deserialize(response).get('Item')
                if _item is not None:
                    return _response(200, _item)
            elif request.method == 'DELETE':
                _table.delete_item(Key=key)
                return _response(204, None)

        # /dbs/{database}/colls/{table}/docs
        elif len(parts) == 5 and parts[-1] == 'docs':
            if request.method == 'POST':
                is_query = headers.get('x-ms-documentdb-isquery') == 'true'
                item = json.loads(request.body)
                if is_query is True:
                    # TODO(x-ms-continuation)
                    # {'initial_headers': {'x-ms-continuation': 'sometoken'}}
                    _items = query_table(
                        item['query'], item['parameters'], parts[-2]
                    )
                    return _response(
                        200, {'Documents': _items}
                    )
                else:
                    _table.put_item(Item=item)
                return _response(201, None)

        # upsert_item() reads the collection
        # /dbs/{database}/colls/{table}
        elif len(parts) == 4 and parts[-2] == 'colls':
            if request.method == 'GET':
                return _response(200, {})

        return _response(404)

    @functools.wraps(f)
    def decorated(*args, **kwargs):
        for method in ['GET', 'POST', 'DELETE', 'PUT']:
            responses.add_callback(
                getattr(responses, method),
                re.compile(r'^https://.*.documents.azure.(com|cn).*'),
                _callback
            )
        return f(*args, **kwargs)
    return decorated
