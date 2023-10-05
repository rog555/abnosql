import functools
import json
import re
import typing as t
from urllib import parse as urlparse

import responses  # type: ignore

import abnosql.mocks.mock_azure_auth as auth
from abnosql.plugins.table.memory import get_table_count
from abnosql import table


KEY_ATTRS: t.Dict[str, t.List[str]] = {}
CRYPTO_ATTRS: t.Dict[str, t.List[str]] = {}
COSMOS_POST_PATCH_VALS = {
    '_rid': '2pFqAMMTYY8BAAAAAAAAAA==',
    '_self': 'dbs/2pFqAA==/colls/2pFqAMMTYY8=/docs/2pFqAMMTYY8BAAAAAAAAAA==/'
}


def set_keyattrs(key_attrs: t.Dict[str, t.List[str]]):
    global KEY_ATTRS
    KEY_ATTRS = key_attrs


def set_crypto_attrs(attrs: t.Dict[str, t.List[str]]):
    global CRYPTO_ATTRS
    CRYPTO_ATTRS = attrs


def mock_cosmos(f):

    def _get_key(headers, key_attrs, doc_id):
        hk = key_attrs[0]
        rk = key_attrs[1] if len(key_attrs) > 1 else None
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
                code, _headers or {
                    'Content-Type': 'application/json'
                }, json.dumps({
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
        global KEY_ATTRS
        key_attrs = KEY_ATTRS.get(table_name)
        if key_attrs is None:
            return _response(404)

        # use memory table to mock cosmos
        global CRYPTO_ATTRS
        crypto_attrs = CRYPTO_ATTRS.get(table_name)
        config = {'key_attrs': key_attrs}
        if crypto_attrs is not None:
            config['crypto'] = {
                'attrs': crypto_attrs,
                'key_attrs': key_attrs
            }
        tb = table(
            table_name,
            config=config,
            database='memory'
        )

        # /dbs/{database}/colls/{table}/docs/{docid}
        if len(parts) == 6 and parts[-2] == 'docs':
            key = _get_key(headers, key_attrs, parts[-1])
            if request.method == 'GET':
                item = tb.get_item(**key)
                if item is not None:
                    return _response(200, item)
            elif request.method == 'DELETE':
                tb.delete_item(**key)
                return _response(204, None)
            elif request.method == 'PATCH':
                data = json.loads(request.body)
                item = {
                    _['path'][1:]: _['value']
                    for _ in data['operations']
                }
                item.update(key)
                item = tb.put_item(item, update=True)
                item.update(COSMOS_POST_PATCH_VALS)
                return _response(200, item)

        # /dbs/{database}/colls/{table}/docs
        elif len(parts) == 5 and parts[-1] == 'docs':
            if request.method == 'POST':
                is_query = headers.get('x-ms-documentdb-isquery') == 'true'
                item = json.loads(request.body)
                if is_query is True:
                    items = tb.query_sql(
                        item['query'],
                        {
                            _['name']: _['value']
                            for _ in item.get('parameters', {})
                        }
                    )
                    headers = {
                        'x-ms-resource-usage': 'documentsCount=%s' % (
                            get_table_count(table_name)
                        )
                    }
                    return _response(
                        200, {'Documents': items}, headers
                    )
                else:
                    item = tb.put_item(item)
                    item.update(COSMOS_POST_PATCH_VALS)
                    return _response(201, item)

        # upsert_item() reads the collection
        # /dbs/{database}/colls/{table}
        elif len(parts) == 4 and parts[-2] == 'colls':
            if request.method == 'GET':
                return _response(200, {})

        return _response(404)

    @functools.wraps(f)
    def decorated(*args, **kwargs):
        auth.mock_auth()
        for method in ['GET', 'POST', 'DELETE', 'PUT', 'PATCH']:
            responses.add_callback(
                getattr(responses, method),
                re.compile(r'^https://.*.documents.azure.(com|cn).*'),
                _callback
            )
        return f(*args, **kwargs)
    return decorated
