import os
import typing as t

from moto import mock_dynamodb2  # type: ignore
import pluggy  # type: ignore

from nosql import plugin
from nosql import table

from tests import common as cmn


@mock_dynamodb2
def test_get_item():
    os.environ['NOSQL_DB'] = 'dynamodb'
    cmn.create_table('hash_range', ['1', '2'], ['a', 'b'])
    tb = table('hash_range')
    assert tb.get_item(hk='1', rk='a') == cmn.item('1', 'a')
    assert tb.get_item(hk='3', rk='a') is None

    cmn.create_table('hash_only', ['1', '2'])
    tb = table('hash_only')
    assert tb.get_item(hk='1') == cmn.item('1')
    assert tb.get_item(hk='3') is None


@mock_dynamodb2
def test_get_item_hook():
    os.environ['NOSQL_DB'] = 'dynamodb'
    cmn.create_table('hash_range', ['1', '2'], ['a', 'b'])
    tb = table('hash_range')

    hookimpl = pluggy.HookimplMarker('nosql.table')
    assert tb.get_item(hk='1', rk='a') == cmn.item('1', 'a')

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
    cmn.create_table('hash_range')
    tb = table('hash_range')
    assert tb.get_item(hk='1', rk='a') is None
    tb.put_item(cmn.item('1', 'a'))
    assert tb.get_item(hk='1', rk='a') == cmn.item('1', 'a')


@mock_dynamodb2
def test_put_items():
    os.environ['NOSQL_DB'] = 'dynamodb'
    cmn.create_table('hash_range')
    tb = table('hash_range')
    items = cmn.items(['1', '2'], ['a', 'b'])
    tb.put_items(items)


@mock_dynamodb2
def test_delete_item():
    os.environ['NOSQL_DB'] = 'dynamodb'
    cmn.create_table('hash_range')
    tb = table('hash_range')
    tb.put_item(cmn.item('1', 'a'))
    assert tb.get_item(hk='1', rk='a') == cmn.item('1', 'a')
    tb.delete_item(hk='1', rk='a')
    assert tb.get_item(hk='1', rk='a') is None
