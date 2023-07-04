from abc import ABCMeta  # type: ignore
from abc import abstractmethod
import os
import re
import typing as t

import pluggy  # type: ignore
import sqlparse  # type: ignore

import nosql.exceptions as ex
from nosql import plugin

hookimpl = pluggy.HookimplMarker('nosql.table')
hookspec = pluggy.HookspecMarker('nosql.table')


class TableSpecs(plugin.PluginSpec):

    @hookspec(firstresult=True)
    def config(self) -> t.Dict:  # type: ignore[empty-body] # noqa E501
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


def validate_statement(statement: str):
    parsed = sqlparse.parse(statement)[0]

    def _extract_non_select_tokens(tokens):
        invalid_tokens = []
        for token in tokens:
            name = token.value.upper()
            if token.is_group:
                invalid_tokens.extend(_extract_non_select_tokens(token))
            elif token.ttype is sqlparse.tokens.DML and name != 'SELECT':
                invalid_tokens.append(name)
            elif token.ttype is sqlparse.tokens.DDL:
                invalid_tokens.append(name)
        return invalid_tokens

    # validate that SELECT is only specified
    invalid_tokens = _extract_non_select_tokens(parsed.tokens)
    invalid_tokens = sorted(set(invalid_tokens))
    if len(invalid_tokens) > 0:
        raise ex.ValidationException('only SELECT is allowed')


def get_sql_params(
    statement: str,
    parameters: t.Dict[str, t.Any],
    param_val: t.Callable,
    replace: t.Optional[str] = None
) -> t.Tuple[str, t.List]:
    # convert @variable to dynamodb ? placeholders
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


def get_dynamodb_param(var, val):
    key = (
        'N' if isinstance(val, float) or isinstance(val, int)
        else 'NULL' if val is None
        else 'BOOL' if isinstance(val, bool)
        else 'S'
    )
    return {key: str(val)}


def get_dynamodb_query_kwargs(
    name: str,
    key: t.Dict[str, t.Any],
    filters: t.Optional[t.Dict[str, t.Any]] = None
) -> t.Dict:
    if len(key) > 2 or len(key) == 0:
        raise ValueError('key length must be 1 or 2')
    if filters is None:
        filters = {}
    _name_pat = re.compile(r'^[a-zA-Z09_-]+$')

    def _validate_key_names(obj):
        return [_ for _ in obj.keys() if not _name_pat.match(_)]

    invalid = sorted(set(
        _validate_key_names(key) + _validate_key_names(filters)
    ))
    if len(invalid):
        raise ValueError('invalid key or filter keys: ' + ', '.join(invalid))

    _values = {
        f':{k}': v
        for k, v in key.items()
    }
    _names = {}
    for k, v in filters.items():
        _names[f'#{k}'] = k
        _values[f':{k}'] = v

    kwargs = {
        'TableName': name,
        'Select': 'ALL_ATTRIBUTES',
        'KeyConditionExpression': ' AND '.join([
            f'{k} = :{k}' for k in key.keys()
        ]),
        'ExpressionAttributeNames': _names,
        'ExpressionAttributeValues': _values
    }
    if len(filters):
        kwargs['FilterExpression'] = ' AND '.join([
            f'{k} = :{k}' for k in filters.keys()
        ])
    return kwargs


def table(
    name: str, config:
    t.Optional[dict] = None,
    database: t.Optional[str] = None
) -> TableBase:
    if database is None:
        database = os.environ.get('NOSQL_DB')
    pm = plugin.get_pm('table')
    module = pm.get_plugin(database)
    if module is None:
        raise ex.PluginException(f'table.{database} plugin not found')
    return module.Table(pm, name, config)
