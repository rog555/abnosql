import os
import typing as t

import boto3  # type: ignore
from moto import mock_dynamodb2  # type: ignore
import pluggy  # type: ignore

from nosql import plugin
from nosql import table


def _item(hk, rk=None):
    item = {
        'hk': hk,
        'num': 5,
        'obj': {
            'foo': 'bar',
            'num': 5,
            'list': [1, 2, 3],
        },
        'list': [1, 2, 3],
        'str': 'str'
    }
    if rk is not None:
        item['rk'] = rk
    return item


def _items(hks=None, rks=None):
    items = []
    for hk in hks or []:
        if rks:
            for rk in rks:
                items.append(_item(hk, rk))
        else:
            items.append(_item(hk))
    return items


def create_table(name, hks=None, rks=None):
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    key_schema = [
        {'AttributeName': 'hk', 'KeyType': 'HASH'}
    ]
    attr_defs = [
        {'AttributeName': 'hk', 'AttributeType': 'S'}
    ]
    if rks is not None:
        key_schema.append({'AttributeName': 'rk', 'KeyType': 'RANGE'})
        attr_defs.append({'AttributeName': 'rk', 'AttributeType': 'S'})
    params = {
        'TableName': name,
        'KeySchema': key_schema,
        'AttributeDefinitions': attr_defs,
        'ProvisionedThroughput': {
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    }
    dynamodb.create_table(**params)
    _table = dynamodb.Table(name)
    if hks:
        items = _items(hks, rks)
        for item in items:
            _table.put_item(Item=item)


@mock_dynamodb2
def test_get_item():
    os.environ['NOSQL_DB'] = 'dynamodb'
    create_table('hash_range', ['1', '2'], ['a', 'b'])
    tb = table('hash_range')
    assert tb.get_item(hk='1', rk='a') == _item('1', 'a')

    create_table('hash_only', ['1', '2'])
    tb = table('hash_only')
    assert tb.get_item(hk='1') == _item('1')
    # assert False


@mock_dynamodb2
def test_get_item_hook():
    os.environ['NOSQL_DB'] = 'dynamodb'
    create_table('hash_range', ['1', '2'], ['a', 'b'])
    tb = table('hash_range')

    hookimpl = pluggy.HookimplMarker('nosql.table')
    assert tb.get_item(hk='1', rk='a') == _item('1', 'a')

    class TableHooks:

        @hookimpl
        def get_item_post(self, table: str, item: t.Dict) -> t.Dict:
            print(f'{table}.get_item_post({item})')
            return {'foo': 'bar'}

    pm = plugin.get_pm('table')
    pm.register(TableHooks())

    assert tb.get_item(hk='1', rk='a') == {'foo': 'bar'}
    pm.unregister(pm.get_plugin('dynamodb'))
    plugin.clear_pms()


@mock_dynamodb2
def test_put_item():
    os.environ['NOSQL_DB'] = 'dynamodb'
    create_table('hash_range')
    tb = table('hash_range')
    assert tb.get_item(hk='1', rk='a') is None
    assert tb.put_item(_item('1', 'a')) is True
    assert tb.get_item(hk='1', rk='a') == _item('1', 'a')


@mock_dynamodb2
def test_put_items():
    os.environ['NOSQL_DB'] = 'dynamodb'
    create_table('hash_range')
    tb = table('hash_range')
    items = _items(['1', '2'], ['a', 'b'])
    assert tb.put_items(items) is True


@mock_dynamodb2
def test_delete_item():
    os.environ['NOSQL_DB'] = 'dynamodb'
    create_table('hash_range')
    tb = table('hash_range')
    assert tb.put_item(_item('1', 'a')) is True
    assert tb.get_item(hk='1', rk='a') == _item('1', 'a')
    assert tb.delete_item(hk='1', rk='a') is True
    assert tb.get_item(hk='1', rk='a') is None
