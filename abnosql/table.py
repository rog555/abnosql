from abc import ABCMeta  # type: ignore
from abc import abstractmethod
import json
import os
import re
import typing as t

import pluggy  # type: ignore

import abnosql.exceptions as ex
from abnosql.kms import kms
from abnosql import plugin

hookimpl = pluggy.HookimplMarker('abnosql.table')
hookspec = pluggy.HookspecMarker('abnosql.table')


class TableSpecs(plugin.PluginSpec):

    @hookspec(firstresult=True)
    def set_config(self, table: str) -> t.Dict:  # type: ignore[empty-body] # noqa E501
        """Hook to set config

        Args:

            table: table name

        Returns:

            dictionary containing config

        """
        pass

    @hookspec(firstresult=True)
    def get_item_post(self, table: str, item: t.Dict) -> t.Dict:  # type: ignore[empty-body] # noqa E501
        """Hook invoked after get_item()

        Args:

            table: table name
            item: dictionary item retrieved from get_item call

        Returns:

            dictionary containing updated item

        """
        pass

    @hookspec
    def put_item_post(self, table: str, item: t.Dict) -> None:  # type: ignore[empty-body] # noqa E501
        """Hook invoked after put_item()

        Args:

            table: table name
            item: dictionary containing partition and range/sort key

        """
        pass

    @hookspec
    def put_items_post(self, table: str, items: t.List[t.Dict]) -> None:  # type: ignore[empty-body] # noqa E501
        """Hook invoked after put_items()

        Args:

            table: table name
            item: list of dictionary items written to table

        """
        pass

    @hookspec
    def delete_item_post(self, table: str, key: t.Dict) -> None:  # type: ignore[empty-body] # noqa E501
        """Hook invoked after delete_item()

        Args:

            table: table name
            key: dictionary of item written to table

        """
        pass


class TableBase(metaclass=ABCMeta):
    @abstractmethod
    def __init__(
        self, pm: plugin.PM, name: str, config: t.Optional[dict] = None
    ) -> None:
        """Instantiate table object

        Args:

            pm: pluggy plugin manager
            name: table name
            config: optional config dict dict
        """
        pass

    @abstractmethod
    def get_item(self, **kwargs) -> t.Dict:
        """Get table/collection item

        Args:

            partition key and range/sort key (if used)

        Returns:

            item dictionary or None if not found

        """
        pass

    @abstractmethod
    def put_item(self, item: t.Dict):
        """Puts table/collection item

        Args:

            item: dictionary

        """
        pass

    @abstractmethod
    def put_items(self, items: t.Iterable[t.Dict]):
        """Puts multiple table/collection items

        Args:

            items: list of item dictionaries

        """
        pass

    @abstractmethod
    def delete_item(self, **kwargs):
        """Deletes table/collection item

        Args:
            partition key and range/sort key (if used)

        """
        pass

    @abstractmethod
    def query(
        self,
        key: t.Dict[str, t.Any],
        filters: t.Optional[t.Dict[str, t.Any]] = None,
        limit: t.Optional[int] = None,
        next: t.Optional[str] = None
    ) -> t.Dict[str, t.Any]:
        """Perform key based query with optional exact match filters

        Args:

            key: dictionary containing partition key and range/sort key
            filters: optional dictionary of key=value to query and filter on
            limit: query limit
            next: pagination token

        Returns:
            dictionary containing 'items' and 'next' pagination token

        """
        pass

    @abstractmethod
    def query_sql(
        self,
        statement: str,
        parameters: t.Optional[t.Dict[str, t.Any]] = None,
        limit: t.Optional[int] = None,
        next: t.Optional[str] = None
    ) -> t.Dict[str, t.Any]:
        """Perform key based query with optional exact match filters

        Args:

            statement: SQL statement to query table
            parameters: optional dictionary containing @key = value placeholders
            limit: query limit
            next: pagination token

        Returns:
            dictionary containing 'items' and 'next' pagination token

        """
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


def kms_encrypt_item(config: t.Dict, item: t.Dict) -> t.Dict:
    kcfg = config.get('kms')
    if kcfg is None:
        return item
    context = {k: item.get(k) for k in kcfg['key_attrs']}
    # encrypt defined attrs
    for attr in kcfg['attrs']:
        val = item.get(attr)
        if val is None:
            continue
        if not isinstance(attr, str):
            val = json.dumps(val)
        item[attr] = kcfg['pm'].encrypt(val, context)
    return item


def kms_decrypt_item(config: t.Dict, item: t.Dict) -> t.Dict:
    kcfg = config.get('kms')
    if not isinstance(kcfg, dict):
        return item
    context = {k: item.get(k) for k in kcfg['key_attrs']}
    # decrypt defined attrs
    for attr in kcfg['attrs']:
        val = item.get(attr)
        if val is None:
            continue
        if not isinstance(attr, str):
            val = json.dumps(val)
        item[attr] = kcfg['pm'].decrypt(val, context)
    return item


def kms_process_query_items(
    config: t.Dict,
    items: t.List[t.Dict]
) -> t.List[t.Dict]:
    # remove encrypted values from items
    kcfg = config.get('kms')
    if not isinstance(kcfg, dict):
        return items
    for i in range(len(items)):
        for attr in kcfg['attrs']:
            items[i].pop(attr, None)
    return items


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
    if not isinstance(config, dict):
        config = {}
    _module = module.Table(pm, name, config)

    # load crypto module
    kcfg = config.get('kms')
    if isinstance(kcfg, dict):
        defaults = {
            'dynamodb': 'aws',
            'cosmosdb': 'azure'
        }
        provider = kcfg.get('provider')
        if database is not None and provider is None:
            provider = defaults.get(database)
        _crypto_module = kms(kcfg, provider)
        config['kms']['pm'] = _crypto_module

        # check required attrs
        missing = [
            _ for _ in ['attrs', 'key_attrs']
            if not isinstance(kcfg, list) or len(kcfg[_]) == 0
        ]
        if len(missing):
            raise ex.ConfigException(
                'kms config missing %s' % ', '.join(missing)
            )

    return _module
