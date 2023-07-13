import functools
import json
import typing as t

import pluggy  # type: ignore
import sqlglot  # type: ignore
from sqlglot.executor import execute  # type: ignore

import abnosql.exceptions as ex
from abnosql.plugin import PM
from abnosql.table import crypto_decrypt_item
from abnosql.table import crypto_encrypt_item
from abnosql.table import crypto_process_query_items
from abnosql.table import get_sql_params
from abnosql.table import quote_str
from abnosql.table import TableBase
from abnosql.table import validate_query_attrs


hookimpl = pluggy.HookimplMarker('abnosql.table')

TABLES: t.Dict = {}


def clear_tables():
    global TABLES
    TABLES = {}


def get_key(**kwargs):
    key = dict(kwargs)
    if len(key) > 2 or len(key) == 0:
        raise ValueError('key length must be 1 or 2')
    return ':'.join(key.values())


def get_table_name(statement: str):
    return str(next(
        sqlglot.parse_one(statement).find_all(  # type: ignore
            sqlglot.exp.Table
        )
    ))


def query_items(
    statement: str,
    items: t.List[t.Dict[str, t.Any]],
    parameters: t.Optional[t.List[t.Dict[str, t.Any]]] = None,
    table_name: t.Optional[str] = None
) -> t.List[t.Dict]:
    parameters = parameters or []

    # find ? dynamodb placeholders
    if '?' in statement:
        _pparams = []
        for pd in parameters:
            _type, _val = list(pd.items())[0]
            if _type == 'S':
                _val = quote_str(_val)
            _pparams.append(_val)
        statement = statement.replace('?', '{}').format(*_pparams)

    # cosmos style placeholders
    elif '@' in statement:
        _nparams = {pd['name']: pd['value'] for pd in parameters}
        for _param, _val in _nparams.items():
            if isinstance(_val, str):
                _val = quote_str(_val)
            statement = statement.replace(_param, str(_val))

    if table_name is None:
        table_name = get_table_name(statement)

    # sqlglot execute can't handle dict or list keys...
    _unpack = {}
    for i in range(len(items)):
        for k, v in items[i].items():
            if type(v) in [dict, list]:
                _unpack[k] = True
                items[i][k] = json.dumps(v)

    # query the data
    resp = execute(statement, tables={table_name: items})
    rows = [
        dict(zip(resp.columns, row))
        for row in resp.rows
    ]

    # unpack
    if len(_unpack):
        for i in range(len(rows)):
            for k in _unpack.keys():
                rows[i][k] = json.loads(rows[i][k])

    return rows


def memory_ex_handler(raise_not_found: t.Optional[bool] = True):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except ex.NotFoundException:
                if raise_not_found:
                    raise
                return None
            except ex.NoSQLException:
                raise
            except Exception as e:
                raise ex.PluginException(e)
        return wrapper
    return decorator


class Table(TableBase):

    def __init__(
        self, pm: PM, name: str, config: t.Optional[dict] = None
    ) -> None:
        self.pm = pm
        self.name = name
        self.set_config(config)
        self.key_attrs = self.config.get('key_attrs', ['id'])
        self.items = self.config.get('items', {})

    @memory_ex_handler()
    def set_config(self, config: t.Optional[dict]):
        if config is None:
            config = {}
        _config = self.pm.hook.set_config(table=self.name)
        if _config:
            config = t.cast(t.Dict, _config)
        self.config = config

    @memory_ex_handler()
    def get_item(self, **kwargs) -> t.Dict:
        key = get_key(**kwargs)
        item = None
        if self.items:
            item = self.items.get(key)
        else:
            global TABLES
            item = TABLES.get(self.name, {}).get(key)
        _item = self.pm.hook.get_item_post(table=self.name, item=item)
        if _item:
            item = _item
        item = crypto_decrypt_item(self.config, item)
        return item

    @memory_ex_handler()
    def put_item(self, item: t.Dict):
        key = ':'.join([item[_] for _ in self.key_attrs])
        item = crypto_encrypt_item(self.config, item)
        if self.items:
            self.items[key] = item
        else:
            global TABLES
            if self.name not in TABLES:
                TABLES[self.name] = {}
            TABLES[self.name][key] = item
        self.pm.hook.put_item_post(table=self.name, item=item)

    @memory_ex_handler()
    def put_items(self, items: t.Iterable[t.Dict]):
        for item in items:
            self.put_item(item)
        self.pm.hook.put_items_post(table=self.name, items=items)

    @memory_ex_handler()
    def delete_item(self, **kwargs):
        key = get_key(**kwargs)
        if self.items:
            self.items.pop(key, None)
        else:
            global TABLES
            TABLES.get(self.name, {}).pop(key, None)
        self.pm.hook.delete_item_post(table=self.name, key=key)

    @memory_ex_handler()
    def query(
        self,
        key: t.Dict[str, t.Any],
        filters: t.Optional[t.Dict[str, t.Any]] = None,
        limit: t.Optional[int] = None,
        next: t.Optional[str] = None
    ) -> t.Dict[str, t.Any]:
        filters = filters or {}
        validate_query_attrs(key, filters)
        parameters = {
            f'@{k}': v
            for k, v in filters.items()
        }
        parameters.update({
            f'@{k}': v
            for k, v in key.items()
        })
        statement = f'SELECT * FROM {self.name}'
        op = 'WHERE'
        for param in parameters.keys():
            statement += f' {op} {self.name}.{param[1:]} = {param}'
            op = 'AND'
        items = self.query_sql(statement)
        items = crypto_process_query_items(self.config, items)
        return {
            'items': items,
            'next': None
        }

    @memory_ex_handler()
    def query_sql(
        self,
        statement: str,
        parameters: t.Optional[t.Dict[str, t.Any]] = None,
        limit: t.Optional[int] = None,
        next: t.Optional[str] = None
    ) -> t.List[t.Dict]:
        parameters = parameters or {}

        def _get_param(var, val):
            return {'name': var, 'value': val}

        (statement, params) = get_sql_params(
            statement, parameters, _get_param
        )
        items = []
        if self.items:
            items = list(self.items.values())
        else:
            global TABLES
            items = list(TABLES.get(self.name, {}).values())
        items = query_items(statement, items, params, self.name)
        items = crypto_process_query_items(self.config, items)
        return items
