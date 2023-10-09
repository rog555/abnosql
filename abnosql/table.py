from abc import ABCMeta  # type: ignore
from abc import abstractmethod
from datetime import datetime
from datetime import timezone
import json
import jsonschema  # type: ignore
import os
import re
import typing as t
from urllib.parse import urlparse
from yaml import safe_load  # type: ignore

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
    def put_item_pre(self, table: str, item: t.Dict) -> t.Dict:  # type: ignore[empty-body] # noqa E501
        """Hook invoked before put_item()

        Args:

            table: table name
            item: dictionary containing partition and range/sort key

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
    def get_item(self, **kwargs) -> t.Optional[t.Dict]:
        """Get table/collection item

        Args:

            partition key and range/sort key (if used)

        Returns:

            item dictionary or None if not found

        """
        pass

    @abstractmethod
    def put_item(
        self,
        item: t.Dict,
        update: t.Optional[bool] = False,
        audit_user: t.Optional[str] = None
    ) -> t.Dict:
        """Puts table/collection item

        Args:

            item: dictionary
            update: perform update/patch - item must already exist
            audit_user: optional user / system ID string to add audit attrs

        Returns:

            item: dictionary of created/updated item

        """
        pass

    @abstractmethod
    def put_items(
        self,
        items: t.Iterable[t.Dict],
        update: t.Optional[bool] = False,
        audit_user: t.Optional[str] = None
    ):
        """Puts multiple table/collection items

        Args:

            items: list of item dictionaries
            update: perform update/patch - items must already exist
            audit_user: user / system ID string to add audit attrs

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
        key: t.Optional[t.Dict[str, t.Any]] = None,
        filters: t.Optional[t.Dict[str, t.Any]] = None,
        limit: t.Optional[int] = None,
        next: t.Optional[str] = None,
        index: t.Optional[str] = None
    ) -> t.Dict[str, t.Any]:
        """Perform key based query with optional exact match filters

        Args:

            key: dictionary containing partition key and range/sort key
            filters: optional dictionary of key=value to query and filter on
            limit: query limit
            next: pagination token
            index: name of index to use (dynamodb only)

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
    """Get and validate statement and parameters

    Args:

        statement: SQL statement to query table
        parameters: optional dictionary containing @key = value placeholders
        param_val: callable to get
        replace: optional placeholder string (eg ?) to replace vars with

    Returns:
        statement, params

    """
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


def quote_str(string):
    # Quotes string
    return "'" + string.translate(
        string.maketrans({
            "'": "\\'"
        })
    ) + "'"


def validate_query_attrs(key: t.Dict, filters: t.Dict):
    """Validate that the query and filter attributes are named correctly

    Args:

        key: key dictionary
        filters: filter dictionary

    """
    # convert @variable to dynamodb ? placeholders
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
    """Validate statement

    Args:

        statement: SQL statement

    """
    # sqlglot can do this (and sqlparse), but lets keep it simple
    tokens = [_.strip() for _ in statement.split(' ') if _.strip() != '']
    if len(tokens) == 0 or tokens[0].upper() != 'SELECT':
        raise ex.ValidationException('statement must start with SELECT')


def get_key_attrs(config: t.Optional[t.Dict] = None) -> list:
    """Get key attributes from ABNOSQL_KEY_ATTRS env var or config

    Args:

        config: config dict

    Returns:

        key_attrs: list of key attribute names

    """
    key_attrs = [
        _ for _ in os.environ.get('ABNOSQL_KEY_ATTRS', '').split(',')
        if _.strip() != ''
    ]
    _key_attrs = config.get('key_attrs') if isinstance(config, dict) else None
    if isinstance(_key_attrs, list):
        key_attrs = _key_attrs
    if len(key_attrs) > 2:
        raise ValueError('must not be more than 2 key_attrs defined')
    return key_attrs


def validate_key_attrs(key_attrs: t.Iterable[str], item: t.Dict):
    """Validate key attributes in either ABNOSQL_KEY_ATTRS env var or config

    Args:

        key_attrs: list of key attribute names
        config: config dict

    """
    if not isinstance(key_attrs, list) or len(key_attrs) == 0:
        raise ex.ValidationException('key_attrs not defined')
    missing = [
        k for k in key_attrs if item.get(k) is None
    ]
    if len(missing):
        raise ex.ValidationException(
            f'key_attrs missing from item: {missing}'
        )
    if len(item) - len(key_attrs) <= 0:
        raise ex.ValidationException(
            'item contains no additional keys beyond key_attrs'
        )


def validate_item(
    config: t.Dict, operation: str, item: t.Dict
):
    schema = config.get(f'{operation}_schema', config.get('schema'))
    if schema is None:
        return
    camel_case = os.environ.get('ABNOSQL_CAMELCASE', 'TRUE') == 'TRUE'
    meta_attr = 'changeMetadata' if camel_case else 'change_metadata'
    name_attr = 'eventName' if camel_case else 'event_name'
    existing_event = item.get(meta_attr, {}).get(name_attr)
    if existing_event == 'REMOVE':
        return
    title = config.get(
        f'{operation}_schema_errmsg',
        config.get('schema_errmsg', 'invalid item')
    )
    schema = safe_load(schema) if isinstance(schema, str) else schema
    validator = jsonschema.Draft7Validator(schema)
    errors = []
    for err in sorted(validator.iter_errors(item), key=str):
        errors.append(err.message)
    if len(errors) > 0:
        raise ex.ValidationException(title, {'errors': errors})


def kms_encrypt_item(config: t.Dict, item: t.Dict) -> t.Dict:
    """Encrypt item values as defined in config

    Each attribute value is encrypted with data key generated each time for both providers:

    - aws_kms uses aws-encryption-sdk
    - azure_kms uses Azure Keyvault RSA CMK to envelope encrypt data key

    Both providers use AESGCM generated data key with AAD/encryption context

    Example config:

        {
            'kms': {
                'key_ids': ['https://foo.vault.azure.net/keys/bar/45e36a1024a04062bd489db0d9004d09'],
                'key_attrs': ['hk', 'rk'],
                'attrs': ['obj', 'str'],
                'key_bytes': b'somekeybytearray'
            }
        }

    Where:
        - key_ids: list of AWS KMS Key ARNs or  or Azure KeyVault identifier (URL to RSA CMK).  This is picked up via `ABNOSQL_KMS_KEYS` env var as comma separated list
        - key_attrs: key attributes in the item for which to the AAD/encryption context is set
        - attrs: attributes to encrypt
        - key_bytes: use your own AESGCM key if specified, otherwise generate one

    Args:

        config: config dictionary
        item: item dict

    Returns:
        item

    """  # noqa: E501
    kcfg = config.get('kms', {})
    if item is None or not kcfg:
        return item
    context = {k: item.get(k) for k in kcfg['key_attrs']}
    # encrypt defined attrs
    for attr in kcfg['attrs']:
        val = item.get(attr)
        if val is None:
            continue
        if not isinstance(val, str):
            val = json.dumps(val)
        item[attr] = kcfg['pm'].encrypt(
            val, context, key=kcfg.get('key_bytes')
        )
    return item


def kms_decrypt_item(config: t.Dict, item: t.Dict) -> t.Dict:
    """Decrypt item as defined in config

    See kms_encrypt_item() for example config:

    Args:

        config: config dictionary
        item: item dict

    Returns:
        item
    """
    kcfg = config.get('kms', {})
    if item is None or not kcfg:
        return item
    context = {k: item.get(k) for k in kcfg['key_attrs']}
    # decrypt defined attrs
    for attr in kcfg['attrs']:
        val = item.get(attr)
        if val is None:
            continue
        val = kcfg['pm'].decrypt(val, context)
        try:
            val = json.loads(val)
        except Exception:
            val = val
        item[attr] = val
    return item


def kms_process_query_items(
    config: t.Dict,
    items: t.List[t.Dict]
) -> t.List[t.Dict]:
    """Remove encrypted attribute/values from items

    Args:

        config: config dictionary
        items: list of item dicts

    Returns:
        items

    """
    kcfg = config.get('kms')
    if not isinstance(kcfg, dict):
        return items
    _items = []
    for item in items:
        for attr in kcfg['attrs']:
            item.pop(attr, None)
        _items.append(item)
    return _items


def add_audit(item: t.Dict, update: bool, user: str) -> t.Dict:
    """Add createdBy + createdDate and/or modifiedBy + modifiedDate to item

    Args:

        item: item dict
        update: bool, true if operation is update otherwise false
        user: user/system ID string

    Returns:
        item

    """
    camel_case = os.environ.get('ABNOSQL_CAMELCASE', 'TRUE') == 'TRUE'
    by_attr = 'By' if camel_case else '_by'
    date_attr = 'Date' if camel_case else '_date'
    dt_iso = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    if update is not True and f'created{by_attr}' not in item:
        item.update({
            f'created{by_attr}': user,
            f'created{date_attr}': dt_iso
        })
    item.update({
        f'modified{by_attr}': user,
        f'modified{date_attr}': dt_iso
    })
    return item


def add_change_meta(item: t.Dict, event_source: str, event_name: str) -> t.Dict:
    """Add changeMetadata object to item containing eventName and eventSource

    Args:

        item: item dict
        event_source: str
        event_name: str - INSERT, MODIFY or REMOVE

    Returns:
        item

    """
    event_name = event_name.upper()
    if event_name not in ['INSERT', 'MODIFY', 'REMOVE']:
        return item
    camel_case = os.environ.get('ABNOSQL_CAMELCASE', 'TRUE') == 'TRUE'
    meta_attr = 'changeMetadata' if camel_case else 'change_metadata'
    source_attr = 'eventSource' if camel_case else 'event_source'
    name_attr = 'eventName' if camel_case else 'event_name'
    # cosmos delete_item() adds REMOVE so don't add if already present in item
    existing_event = item.get(meta_attr, {}).get(name_attr)
    if existing_event == 'REMOVE':
        return item
    item.update({
        meta_attr: {
            source_attr: event_source,
            name_attr: event_name
        }
    })
    return item


def check_exists_enabled(config):
    return config.get(
        'check_exists',
        os.environ.get('ABNOSQL_CHECK_EXISTS', 'FALSE') == 'TRUE'
    ) is True


def check_exists(obj: TableBase, operation: str, item: dict):
    if len(obj.key_attrs) == 0 or obj.check_exists is False:  # type: ignore
        return item
    key = {
        k: item.get(k) for k in obj.key_attrs  # type: ignore
    } if item is not None else None
    if operation == 'get' and item is None:
        raise ex.NotFoundException('item not found')
    elif operation == 'create' and key is not None:
        # can be overridden if defined in item
        if item.pop('abnosql_check_exists', None) is False:
            return item
        key['abnosql_check_exists'] = False
        if obj.get_item(**key) is not None:
            raise ex.ExistsException('item already exists')
    elif operation == 'update' and key is not None:
        if obj.get_item(**key) is None:
            raise ex.NotFoundException('item not found')
    elif operation == 'delete' and key is not None:
        if obj.get_item(**key) is None:
            raise ex.NotFoundException('item not found')
    return item


def parse_connstr():
    connstr = os.environ.get('ABNOSQL_DB')
    if connstr:
        return urlparse(connstr)
    return None


def table(
    name: str,
    config: t.Optional[dict] = None,
    database: t.Optional[str] = None
) -> TableBase:
    """Create table object

    Args:

        name: table name
        config: optional config
        database: optional database

    Returns:
        TableBase object

    """
    if database is None:
        p = parse_connstr()
        database = p.scheme or p.path if p else None

    # infer database from available env vars
    # aws: https://docs.aws.amazon.com/lambda/latest/dg/configuration-envvars.html  # noqa
    # azure: https://learn.microsoft.com/en-us/azure/azure-functions/functions-app-settings#azure_functions_environment  # noqa
    if database is None:
        defaults = {
            'AWS_DEFAULT_REGION': 'dynamodb',
            'FUNCTIONS_WORKER_RUNTIME': 'cosmos'
        }
        for envvar, _database in defaults.items():
            if os.environ.get(envvar) is not None:
                database = _database
                break

    if database is None:
        raise ex.PluginException('table plugin database not defined')

    pm = plugin.get_pm('table')
    module = pm.get_plugin(database)
    if module is None:
        raise ex.PluginException(f'table.{database} plugin not found')
    if hasattr(module, 'MISSING_DEPS'):
        raise ex.PluginException(
            f'table.{database} plugin missing dependencies'
        )
    if not isinstance(config, dict):
        config = {}
    _module = module.Table(pm, name, config)

    # load crypto module
    kcfg = config.get('kms')
    if isinstance(kcfg, dict):
        defaults = {
            'dynamodb': 'aws',
            'cosmos': 'azure'
        }
        provider = kcfg.get('provider')
        if database is not None and provider is None:
            provider = defaults.get(database)
        _kms_module = kms(kcfg, provider)
        config['kms']['pm'] = _kms_module

        if 'key_attrs' not in kcfg:
            config['kms']['key_attrs'] = get_key_attrs(config)

        if 'session' in config and 'session' not in kcfg:
            # aws_encryption_sdk uses botocore session
            config['kms']['session'] = config['session']._session

        # check required attrs
        missing = [
            _ for _ in ['attrs', 'key_attrs']
            if not isinstance(kcfg.get(_), list) or len(kcfg[_]) == 0
        ]
        if len(missing):
            raise ex.ConfigException(
                'kms config missing %s' % ', '.join(missing)
            )

    return _module
