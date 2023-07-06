from abc import ABCMeta  # type: ignore
from abc import abstractmethod
import os
import re
import typing as t

import pluggy  # type: ignore

import abnosql.exceptions as ex
from abnosql import plugin

hookimpl = pluggy.HookimplMarker('abnosql.table')
hookspec = pluggy.HookspecMarker('abnosql.table')


class TableSpecs(plugin.PluginSpec):

    @hookspec(firstresult=True)
    def set_config(self, table: str) -> t.Dict:  # type: ignore[empty-body] # noqa E501
        pass

    @hookspec(firstresult=True)
    def get_item_post(self, table: str, item: t.Dict) -> t.Dict:  # type: ignore[empty-body] # noqa E501
        pass

    @hookspec
    def put_item_post(self, table: str, item: t.Dict) -> None:  # type: ignore[empty-body] # noqa E501
        pass

    @hookspec
    def put_items_post(self, table: str, items: t.List[t.Dict]) -> None:  # type: ignore[empty-body] # noqa E501
        pass

    @hookspec
    def delete_item_post(self, table: str, key: t.Dict) -> None:  # type: ignore[empty-body] # noqa E501
        pass


class TableBase(metaclass=ABCMeta):
    @abstractmethod
    def __init__(
        self, pm: plugin.PM, name: str, config: t.Optional[dict] = None
    ) -> None:
        pass

    @abstractmethod
    def get_item(self, **kwargs) -> t.Dict:
        pass

    @abstractmethod
    def put_item(self, item: t.Dict):
        pass

    @abstractmethod
    def put_items(self, items: t.Iterable[t.Dict]):
        pass

    @abstractmethod
    def delete_item(self, **kwargs):
        pass

    @abstractmethod
    def query(
        self,
        key: t.Dict[str, t.Any],
        filters: t.Optional[t.Dict[str, t.Any]] = None,
        limit: t.Optional[int] = None,
        next: t.Optional[str] = None
    ) -> t.Dict[str, t.Any]:
        pass

    @abstractmethod
    def query_sql(
        self,
        statement: str,
        parameters: t.Optional[t.Dict[str, t.Any]] = None,
        limit: t.Optional[int] = None,
        next: t.Optional[str] = None
    ) -> t.Dict[str, t.Any]:
        pass


def get_sql_params(
    statement: str,
    parameters: t.Dict[str, t.Any],
    param_val: t.Callable,
    replace: t.Optional[str] = None
) -> t.Tuple[str, t.List]:
    # convert @variable to dynamodb ? placeholders
    validate_statement(statement)
    vars = list(re.findall(r'\@[a-zA-Z0-9_.-]+', statement))
    params = []
    _missing = {}
    for var in vars:
        if var not in parameters:
            _missing[var] = True
        else:
            val = parameters[var]
            params.append(param_val(var, val))
    for var in parameters.keys():
        if var not in vars:
            _missing[var] = True
    missing = sorted(_missing.keys())
    if len(missing):
        raise ex.ValidationException(
            'missing parameters: ' + ', '.join(missing)
        )
    if isinstance(replace, str):
        for var in parameters.keys():
            statement = statement.replace(var, replace)
    return (statement, params)


def quote_str(str):
    return "'" + str.translate(
        str.maketrans({
            "'": "\\'"
        })
    ) + "'"


def validate_query_attrs(key: t.Dict, filters: t.Dict):
    _name_pat = re.compile(r'^[a-zA-Z09_-]+$')

    def _validate_key_names(obj):
        return [_ for _ in obj.keys() if not _name_pat.match(_)]

    invalid = sorted(set(
        _validate_key_names(key) + _validate_key_names(filters)
    ))
    if len(invalid):
        raise ex.ValidationException(
            'invalid key or filter keys: ' + ', '.join(invalid)
        )


def validate_statement(statement: str):
    # sqlglot can do this (and sqlparse), but lets keep it simple
    tokens = [_.strip() for _ in statement.split(' ') if _.strip() != '']
    if len(tokens) == 0 or tokens[0].upper() != 'SELECT':
        raise ex.ValidationException('statement must start with SELECT')


def table(
    name: str,
    config: t.Optional[dict] = None,
    database: t.Optional[str] = None
) -> TableBase:
    if database is None:
        database = os.environ.get('ABNOSQL_DB')
    pm = plugin.get_pm('table')
    module = pm.get_plugin(database)
    if module is None:
        raise ex.PluginException(f'table.{database} plugin not found')
    return module.Table(pm, name, config)
