import functools
import json
import re
from urllib import parse as urlparse

import boto3  # type: ignore
import responses  # type: ignore

import nosql.mocks.mock_common as cmn
from nosql.plugins.table.dynamodb import deserialize
from nosql.table import get_dynamodb_query_kwargs


def mock_cosmos(table_keys, db=None):

    def _get_key(headers, table_name, params, doc_id=None):
        schema_keys = table_keys[table_name]
        _part_keys = headers.get('x-ms-documentdb-partitionkey')
        _part_key = schema_keys[0]
        if isinstance(_part_keys, str) and len(_part_keys) > 0:
            _part_val = json.loads(_part_keys)[0]
        else:
            _part_val = params[_part_key]
        key = {
            _part_key: _part_val
        }
        if len(schema_keys) > 1:
            key[schema_keys[1]] = params.get(schema_keys[1], doc_id)
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
        print(f'REQ: {request.method} {path} H: {headers} B: {request.body}')

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
        if table_name not in table_keys:
            return _response(404)

        # use moto dynamodb to mock cosmos :-/
        _table = boto3.resource('dynamodb').Table(table_name)

        # /dbs/{database}/colls/{table}/docs/{docid}
        if len(parts) == 6 and parts[-2] == 'docs':
            key = _get_key(headers, table_name, {}, parts[-1])
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
                    _items = []

                    # TODO(x-ms-continuation)
                    # {'initial_headers': {'x-ms-continuation': 'sometoken'}}
                    # use sqlite as query_sql() backend
                    if db is not None:
                        _items = cmn.sqlite3_query(
                            db, item['query'], item['parameters']
                        )
                    # use dynamodb for query() backend
                    else:
                        _params = {
                            kv['name'].replace('@', ''): kv['value']
                            for kv in item['parameters']
                        }
                        _key = _get_key(headers, table_name, _params)
                        _filters = {
                            k: v for k, v in _params.items() if k not in _key
                        }
                        _resp = _table.query(**get_dynamodb_query_kwargs(
                            table_name, _key, _filters
                        ))
                        _items = _resp.get('Items', [])
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

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for method in ['GET', 'POST', 'DELETE', 'PUT']:
                responses.add_callback(
                    getattr(responses, method),
                    re.compile(r'^https://.*.documents.azure.(com|cn).*'),
                    _callback
                )
        return wrapper

    return decorator
