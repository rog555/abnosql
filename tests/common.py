import typing as t

import boto3  # type: ignore
import pluggy  # type: ignore

from nosql import plugin
from nosql import table


def item(hk, rk=None):
    _item = {
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
        _item['rk'] = rk
    return _item


def items(hks=None, rks=None):
    _items = []
    for hk in hks or []:
        if rks:
            for rk in rks:
                _items.append(item(hk, rk))
        else:
            _items.append(item(hk))
    return _items


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
        _items = items(hks, rks)
        for _item in _items:
            _table.put_item(Item=_item)


def test_get_item(config=None):
    tb = table('hash_range', config)
    create_table('hash_range', ['1', '2'], ['a', 'b'])
    assert tb.get_item(hk='1', rk='a') == item('1', 'a')
    assert tb.get_item(hk='3', rk='a') is None

    create_table('hash_only', ['1', '2'])
    tb = table('hash_only', config)
    assert tb.get_item(hk='1') == item('1')
    assert tb.get_item(hk='3') is None


def test_get_item_hook(config=None):
    tb = table('hash_range', config)
    create_table('hash_range', ['1', '2'], ['a', 'b'])

    hookimpl = pluggy.HookimplMarker('nosql.table')
    assert tb.get_item(hk='1', rk='a') == item('1', 'a')

    class TableHooks:

        @hookimpl
        def get_item_post(self, table: str, item: t.Dict) -> t.Dict:
            return {'foo': 'bar'}

    pm = plugin.get_pm('table')
    pm.register(TableHooks())

    assert tb.get_item(hk='1', rk='a') == {'foo': 'bar'}
    pm.unregister(pm.get_plugin('dynamodb'))
    plugin.clear_pms()


def test_put_item(config=None):
    tb = table('hash_range', config)
    create_table('hash_range')
    assert tb.get_item(hk='1', rk='a') is None
    tb.put_item(item('1', 'a'))
    assert tb.get_item(hk='1', rk='a') == item('1', 'a')


def test_put_items(config=None):
    tb = table('hash_range', config)
    create_table('hash_range')
    tb.put_items(items(['1', '2'], ['a', 'b']))


def test_delete_item(config=None):
    tb = table('hash_range', config)
    create_table('hash_range')
    tb.put_item(item('1', 'a'))
    assert tb.get_item(hk='1', rk='a') == item('1', 'a')
    tb.delete_item(hk='1', rk='a')
    assert tb.get_item(hk='1', rk='a') is None


def test_query(config=None):
    tb = table('hash_range', config)
    create_table('hash_range', ['1', '2'], ['a', 'b'])
    response = tb.query(
        {'hk': '1'},
        {'rk': 'a'}
    )
    assert response == {
        'items': items(['1'], ['a']),
        'next': None
    }


def test_query_sql(config=None):
    create_table('hash_range', ['1', '2'], ['a', 'b'])
    tb = table('hash_range', config)
    response = tb.query_sql(
        'SELECT * FROM hash_range WHERE hk = @hk AND num > @num',
        {'@hk': '1', '@num': 4}
    )
    assert response == {
        'items': items(['1'], ['a', 'b']),
        'next': None
    }
