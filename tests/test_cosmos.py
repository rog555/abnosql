from base64 import b64encode
import json
import os
import re
from urllib import parse as urlparse

import boto3  # type: ignore
from moto import mock_dynamodb2  # type: ignore
import responses  # type: ignore

from nosql.plugins.table.dynamodb import deserialize

from tests import common as cmn
from tests.mock_boto3 import mock_boto3


def mock_cosmos(table_keys, db=None):

    def _get_key(headers, table_name, doc_id):
        _part_keys = headers.get('x-ms-documentdb-partitionkey')
        _part_key = (
            json.loads(_part_keys)[0] if isinstance(_part_keys, str)
            and len(_part_keys) > 0 else None
        )
        schema_keys = table_keys[table_name]
        key = {
            schema_keys[0]: doc_id
        }
        if _part_key is not None and len(schema_keys) > 1:
            key[schema_keys[1]] = _part_key
        return key

    def _callback(request):
        path = urlparse.urlsplit(request.url).path
        # _params = dict(urlparse.parse_qsl(
        #     urlparse.urlsplit(request.url).query
        # ))
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
            key = _get_key(headers, table_name, parts[-1])
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
                    _items = cmn.sqlite3_query(
                        item['query'], item['parameters']
                    )
                    # TODO(x-ms-continuation)
                    # {'initial_headers': {'x-ms-continuation': 'sometoken'}}
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

    for method in ['GET', 'POST', 'DELETE', 'PUT']:
        responses.add_callback(
            getattr(responses, method),
            re.compile(r'^https://.*.documents.azure.(com|cn).*'),
            _callback
        )


def setup():
    os.environ['NOSQL_DB'] = 'cosmos'
    os.environ['NOSQL_COSMOS_ACCOUNT'] = 'foo'
    os.environ['NOSQL_COSMOS_CREDENTIAL'] = b64encode(
        'mycredential'.encode('utf-8')
    ).decode()
    os.environ['NOSQL_COSMOS_DATABASE'] = 'bar'
    mock_cosmos({'hash_range': ['hk', 'rk'], 'hash_only': ['hk']})


@mock_dynamodb2
@responses.activate
def test_get_item():
    setup()
    cmn.test_get_item()


@mock_dynamodb2
@responses.activate
def test_put_item():
    setup()
    cmn.test_put_item()


@mock_dynamodb2
@responses.activate
def test_put_items():
    setup()
    cmn.test_put_items()


@mock_dynamodb2
@responses.activate
def test_delete_item():
    setup()
    cmn.test_delete_item()


@mock_boto3
@responses.activate
def test_query():
    setup()
    cmn.test_query()
