import os
import typing as t

import pluggy  # type: ignore
import pytest

import abnosql.exceptions as ex
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


def validate_change_meta(item, event_name):
    if os.environ.get('ABNOSQL_COSMOS_CHANGE_META', 'TRUE') != 'TRUE':
        return item
    _item = item.copy()
    change_meta = _item.pop('changeMetadata', None)
    database = os.environ.get('ABNOSQL_DB')
    if database is not None and database.startswith('cosmos'):
        assert isinstance(change_meta, dict)
        assert change_meta.get('eventSource', '').startswith('hash_')
        assert change_meta.get('eventName') == event_name
    else:
        assert change_meta is None
    return _item


def validate_change_meta_response(response, event_name):
    for i in range(len(response['items'])):
        response['items'][i] = validate_change_meta(
            response['items'][i], event_name
        )
    return response


def test_get_item(config=None, tables=None):
    actual_item = None
    tb = None
    if tables is None or 'hash_range' in tables:
        os.environ['ABNOSQL_KEY_ATTRS'] = 'hk,rk'
        tb = table('hash_range', config)
        assert tb.get_item(hk='1', rk='a') is None
        tb.put_item(item('1', 'a'))
        actual_item = tb.get_item(hk='1', rk='a')
        expected_item = item('1', 'a')
        actual_item = validate_change_meta(actual_item, 'INSERT')
        assert actual_item == expected_item

    if tables is None or 'hash_only' in tables:
        os.environ['ABNOSQL_KEY_ATTRS'] = 'hk'
        tb = table('hash_only', config)
        assert tb.get_item(hk='1') is None
        tb.put_item(item('1'))
        actual_item = tb.get_item(hk='1')
        expected_item = item('1')
        actual_item = validate_change_meta(actual_item, 'INSERT')
        assert actual_item == expected_item

    return tb


def test_check_exists(config=None):
    config = config or {}
    config.update({'key_attrs': ['hk', 'rk'], 'check_exists': True})
    tb = table('hash_range', config)

    with pytest.raises(ex.NotFoundException) as e:
        tb.get_item(hk='1', rk='a')
    assert str(e.value) == 'item not found'

    with pytest.raises(ex.NotFoundException) as e:
        tb.put_item(item('1', 'a'), update=True)
    assert str(e.value) == 'item not found'

    with pytest.raises(ex.NotFoundException) as e:
        tb.delete_item(hk='1', rk='a')
    assert str(e.value) == 'item not found'

    tb.put_item(item('1', 'a'))

    # check can override
    tb.put_item({**item('1', 'a'), **{'abnosql_check_exists': False}})

    assert tb.get_item(hk='1', rk='a') is not None

    with pytest.raises(ex.ExistsException) as e:
        tb.put_item(item('1', 'a'), update=False)
    assert str(e.value) == 'item already exists'


def test_validate_item(_config=None):
    _config = _config or {}
    schema1 = '''
type: object
properties:
  hk:
    type: string
  rk:
    type: string
  obj2:
    type: object
required: [hk, rk, obj2]
'''
    schema2 = '''
type: object
properties:
  hk:
    type: string
  rk:
    type: string
  str2:
    type: string
required: [hk, rk, str2]
'''
    config = _config.copy()
    config.update({
        'key_attrs': ['hk', 'rk'],
        'create_schema': schema1,
        'create_schema_errmsg': 'create failed',
        'update_schema': schema2,
        'update_schema_errmsg': 'update failed'
    })
    tb = table('hash_range', config)
    with pytest.raises(ex.ValidationException) as e:
        tb.put_item(item('1', 'a'))
    assert e.value.title == 'create failed'
    assert e.value.detail == {'errors': ["'obj2' is a required property"]}
    assert e.value.status == 400

    tb.put_item({**item('1', 'a'), **{'obj2': {}}})
    with pytest.raises(ex.ValidationException) as e:
        tb.put_item(item('1', 'a'), update=True)
    assert e.value.title == 'update failed'
    assert e.value.detail == {'errors': ["'str2' is a required property"]}

    tb.put_item({**item('1', 'a'), **{'str2': 'foo'}}, update=True)

    # single schema for both create and update, with single errmsg
    config = _config.copy()
    config.update({
        'key_attrs': ['hk', 'rk'],
        'schema': schema1,
        'schema_errmsg': 'invalid thing'
    })
    tb = table('hash_range', config)
    with pytest.raises(ex.ValidationException) as e:
        tb.put_item(item('1', 'a'))
    assert e.value.title == 'invalid thing'
    tb.put_item({**item('1', 'a'), **{'obj2': {}}})
    with pytest.raises(ex.ValidationException) as e:
        tb.put_item(item('1', 'a'), update=True)
    assert e.value.title == 'invalid thing'

    # single schema with default errmsg (key_attrs not needed as no update)
    config = _config.copy()
    config.update({
        'schema': schema1,
    })
    tb = table('hash_range', config)
    with pytest.raises(ex.ValidationException) as e:
        tb.put_item(item('1', 'a'))
    assert e.value.title == 'invalid item'

    # check problem
    assert e.value.to_problem() == {
        'title': 'invalid item',
        'detail': {'errors': ["'obj2' is a required property"]},
        'status': 400,
        'type': None
    }

    # check can delete with no validation (cosmos)
    tb.delete_item(hk='1', rk='a')


def test_put_item(config=None):
    tb = table('hash_range', config)
    assert tb.get_item(hk='1', rk='a') is None
    tb.put_item(item('1', 'a'))
    actual_item = validate_change_meta(tb.get_item(hk='1', rk='a'), 'INSERT')
    assert actual_item == item('1', 'a')


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
    validate_change_meta(item1, 'INSERT')

    tb.put_item(
        {'hk': '1', 'rk': 'a', 'str': 'STR'},
        update=True,
        audit_user='bar'
    )
    item2 = tb.get_item(hk='1', rk='a')
    assert item2['createdBy'] == 'foo'
    assert item2['modifiedBy'] == 'bar'
    assert item2['createdDate'] == item1['createdDate']
    assert item2['modifiedDate'] >= item2['createdDate']
    assert item2['str'] == 'STR'

    validate_change_meta(item2, 'MODIFY')
    os.environ.pop('ABNOSQL_KEY_ATTRS')


def test_audit_callback(config=None):

    events = []

    def _callback(table_name, dt_iso, operation, key, audit_user):
        assert dt_iso.startswith('20') and dt_iso.endswith('Z')
        events.append(','.join([
            table_name, '<date>', operation, str(key), audit_user
        ]))

    config = config or {}
    os.environ['ABNOSQL_KEY_ATTRS'] = 'hk,rk'
    config.update({
        'audit_user': 'someuser',
        'audit_callback': _callback
    })
    tb = table('hash_range', config)
    assert tb.get_item(hk='1', rk='a') is None

    tb.put_item(item('1', 'a'))

    tb.get_item(hk='1', rk='a')

    tb.put_item({'hk': '1', 'rk': 'a', 'foo': 'bar'}, update=True)

    tb.delete_item(hk='1', rk='a')

    actual = '\n'.join(events)
    print(actual)

    expected = '''hash_range,<date>,create,hk=1;rk=a,someuser
hash_range,<date>,get,hk=1;rk=a,someuser
hash_range,<date>,update,hk=1;rk=a,someuser
hash_range,<date>,delete,hk=1;rk=a,someuser'''

    assert actual == expected


def test_update_item(config=None):
    config = config or {}
    config.update({
        'key_attrs': ['hk', 'rk'],
        'put_get': True  # firestore set/update don't return item
    })
    tb = table('hash_range', config)
    item1 = item('1', 'a')
    assert tb.get_item(hk='1', rk='a') is None
    assert validate_change_meta(
        tb.put_item(item1.copy()), 'INSERT'
    ) == item1
    assert validate_change_meta(
        tb.get_item(hk='1', rk='a'), 'INSERT'
    ) == item1

    # test update
    item2 = {'hk': '1', 'rk': 'a', 'str': 'STR', 'num': 6}
    item3 = item1.copy()
    item3.update(item2)
    assert validate_change_meta(
        tb.put_item(item2.copy(), update=True), 'MODIFY'
    ) == item3
    assert validate_change_meta(
        tb.get_item(hk='1', rk='a'), 'MODIFY'
    ) == item3


def test_put_items(config=None):
    tb = table('hash_range', config)
    tb.put_items(items(['1', '2'], ['a', 'b']))
    validate_change_meta(
        tb.get_item(hk='1', rk='a'), 'INSERT'
    ) == item('1', 'a')


def test_delete_item(config=None):
    tb = table('hash_range', config)
    tb.put_item(item('1', 'a'))
    assert validate_change_meta(
        tb.get_item(hk='1', rk='a'), 'INSERT'
    ) == item('1', 'a')
    tb.delete_item(hk='1', rk='a')
    assert tb.get_item(hk='1', rk='a') is None


def test_hooks(config=None):
    config = config or {}
    hookimpl = pluggy.HookimplMarker('abnosql.table')

    class TableHooks:

        def __init__(self, table) -> None:
            self.called: t.Dict = {}
            self.table = table

        @hookimpl
        def set_config(self, table: str) -> t.Dict:
            self.called['set_config'] = True
            assert self.table == table
            config.update({'a': 'b', 'key_attrs': ['hk', 'rk']})
            return config

        @hookimpl
        def get_item_pre(self, table: str, key: t.Dict):  # noqa E501
            self.called['get_item_pre'] = True
            assert self.table == table

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

    tb = table('hash_range', config)

    assert 'set_config' in hooks.called
    assert tb.config['a'] == 'b'
    assert tb.config['key_attrs'] == ['hk', 'rk']

    tb.put_item(item('1', 'a'))
    assert 'put_item_pre' in hooks.called
    assert 'put_item_post' in hooks.called

    assert tb.get_item(hk='1', rk='a') == {'foo': 'bar'}
    assert 'get_item_pre' in hooks.called
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
    response = validate_change_meta_response(response, 'INSERT')
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
    validate_change_meta_response(response, 'INSERT')
    assert response == {
        'items': items(['1'], ['a', 'b']),
        'next': None
    }


def test_query_scan(config=None):
    tb = table('hash_range', config)
    tb.put_items(items(['1', '2'], ['a', 'b']))
    response = tb.query()
    response = validate_change_meta_response(response, 'INSERT')
    assert response['items'] == items(['1', '2'], ['a', 'b'])


def test_query_pagination(config=None):
    tb = table('hash_range', config)
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
