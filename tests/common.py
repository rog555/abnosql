import os
import typing as t

import pluggy  # type: ignore

from abnosql import plugin
from abnosql import table


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


def test_get_item(config=None, tables=None):
    if tables is None or 'hash_range' in tables:
        tb = table('hash_range', config)
        assert tb.get_item(hk='1', rk='a') is None
        tb.put_item(item('1', 'a'))
        assert tb.get_item(hk='1', rk='a') == item('1', 'a')

    if tables is None or 'hash_only' in tables:
        tb = table('hash_only', config)
        assert tb.get_item(hk='1') is None
        tb.put_item(item('1'))
        assert tb.get_item(hk='1') == item('1')


def test_put_item(config=None):
    tb = table('hash_range', config)
    assert tb.get_item(hk='1', rk='a') is None
    tb.put_item(item('1', 'a'))
    assert tb.get_item(hk='1', rk='a') == item('1', 'a')


def test_put_item_audit(config=None):
    # also test env var vs config
    os.environ['ABNOSQL_KEY_ATTRS'] = 'hk,rk'
    tb = table('hash_range', config)

    tb.put_item(item('1', 'a'), audit_user='foo')
    item1 = tb.get_item(hk='1', rk='a')
    assert item1['createdBy'] == 'foo'
    assert item1['modifiedBy'] == 'foo'
    assert item1['createdDate'].startswith('20')
    assert item1['modifiedDate'] == item1['createdDate']

    item2 = tb.put_item(
        {'hk': '1', 'rk': 'a', 'str': 'STR'},
        update=True,
        audit_user='bar'
    )
    assert item2['createdBy'] == 'foo'
    assert item2['modifiedBy'] == 'bar'
    assert item2['createdDate'] == item1['createdDate']
    assert item2['modifiedDate'] >= item2['createdDate']
    assert item2['str'] == 'STR'
    os.environ.pop('ABNOSQL_KEY_ATTRS')


def test_update_item(config=None):
    config = config or {}
    config.update({'key_attrs': ['hk', 'rk']})
    tb = table('hash_range', config)
    item1 = item('1', 'a')
    assert tb.get_item(hk='1', rk='a') is None
    assert tb.put_item(item1) == item1
    assert tb.get_item(hk='1', rk='a') == item1

    # test update
    item2 = {'hk': '1', 'rk': 'a', 'str': 'STR', 'num': 6}
    item3 = item1.copy()
    item3.update(item2)
    assert tb.put_item(item2, update=True) == item3
    assert tb.get_item(hk='1', rk='a') == item3


def test_put_items(config=None):
    tb = table('hash_range', config)
    tb.put_items(items(['1', '2'], ['a', 'b']))
    assert tb.get_item(hk='1', rk='a') == item('1', 'a')


def test_delete_item(config=None):
    tb = table('hash_range', config)
    tb.put_item(item('1', 'a'))
    assert tb.get_item(hk='1', rk='a') == item('1', 'a')
    tb.delete_item(hk='1', rk='a')
    assert tb.get_item(hk='1', rk='a') is None


def test_hooks(config=None):
    hookimpl = pluggy.HookimplMarker('abnosql.table')

    class TableHooks:

        def __init__(self, table) -> None:
            self.called: t.Dict = {}
            self.table = table

        @hookimpl
        def set_config(self, table: str) -> t.Dict:
            self.called['set_config'] = True
            assert self.table == table
            return {'a': 'b', 'key_attrs': ['hk', 'rk']}

        @hookimpl
        def get_item_post(self, table: str, item: t.Dict) -> t.Dict:  # noqa E501
            self.called['get_item_post'] = True
            assert self.table == table
            return {'foo': 'bar'}

        @hookimpl
        def put_item_pre(self, table: str, item: t.Dict):
            assert self.table == table
            self.called['put_item_pre'] = True
            return item

        @hookimpl
        def put_item_post(self, table: str, item: t.Dict):
            assert self.table == table
            self.called['put_item_post'] = True

        @hookimpl
        def put_items_post(self, table: str, items: t.Iterable[t.Dict]):  # noqa E501
            assert self.table == table
            self.called['put_items_post'] = True

        @hookimpl
        def delete_item_post(self, table: str, key: t.Dict):  # noqa E501
            assert self.table == table
            self.called['delete_item_post'] = True

    hooks = TableHooks('hash_range')
    pm = plugin.get_pm('table')
    pm.register(hooks)

    tb = table('hash_range')

    assert 'set_config' in hooks.called
    assert tb.config == {'a': 'b', 'key_attrs': ['hk', 'rk']}

    tb.put_item(item('1', 'a'))
    assert 'put_item_pre' in hooks.called
    assert 'put_item_post' in hooks.called

    assert tb.get_item(hk='1', rk='a') == {'foo': 'bar'}
    assert 'get_item_post' in hooks.called

    tb.put_items(items(['1', '2'], ['a', 'b']))
    assert 'put_items_post' in hooks.called

    tb.delete_item(hk='1', rk='a')
    assert 'delete_item_post' in hooks.called

    plugin.clear_pms()


def test_query(config=None, return_response=False):
    tb = table('hash_range', config)
    tb.put_items(items(['1', '2'], ['a', 'b']))
    response = tb.query(
        {'hk': '1'},
        {'rk': 'a'}
    )
    if return_response is True:
        return response
    assert response == {
        'items': items(['1'], ['a']),
        'next': None
    }


def test_query_sql(config=None, return_response=False):
    tb = table('hash_range', config)
    tb.put_items(items(['1', '2'], ['a', 'b']))
    response = tb.query_sql(
        'SELECT * FROM hash_range '
        + 'WHERE hash_range.hk = @hk AND hash_range.num > @num',
        {'@hk': '1', '@num': 4}
    )
    if return_response is True:
        return response
    assert response == {
        'items': items(['1'], ['a', 'b']),
        'next': None
    }


def test_query_scan(config=None):
    tb = table('hash_range', config)
    tb.put_items(items(['1', '2'], ['a', 'b']))
    response = tb.query()
    assert response['items'] == items(['1', '2'], ['a', 'b'])


def test_query_pagination(config=None):
    tb = table('hash_range')
    _items = items(['1', '2'], ['a', 'b'])
    tb.put_items(_items)
    response = tb.query(limit=1)
    assert response['items'] == [_items[0]]
    next = response['next']
    assert isinstance(next, str) and len(next) > 0
    response = tb.query(limit=1, next=next)
    assert response['items'] == [_items[1]]
    response = tb.query(limit=2, next=response['next'])
    assert response['items'] == _items[2:4]
    assert response['next'] is None
