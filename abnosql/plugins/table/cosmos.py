import functools
import logging
import os
import time

import typing as t

import pluggy  # type: ignore

import abnosql.exceptions as ex
from abnosql.plugin import PM
from abnosql.table import add_audit
from abnosql.table import add_change_meta
from abnosql.table import check_exists
from abnosql.table import check_exists_enabled
from abnosql.table import get_key_attrs
from abnosql.table import get_sql_params
from abnosql.table import kms_decrypt_item
from abnosql.table import kms_encrypt_item
from abnosql.table import kms_process_query_items
from abnosql.table import parse_connstr
from abnosql.table import TableBase
from abnosql.table import validate_item
from abnosql.table import validate_key_attrs
from abnosql.table import validate_query_attrs

hookimpl = pluggy.HookimplMarker('abnosql.table')

try:
    from azure.cosmos import CosmosClient  # type: ignore
    from azure.cosmos.exceptions import CosmosHttpResponseError  # type: ignore
    from azure.cosmos.exceptions import CosmosResourceNotFoundError  # type: ignore # noqa
    from azure.identity import DefaultAzureCredential  # type: ignore
except ImportError:
    MISSING_DEPS = True


def cosmos_ex_handler(raise_not_found: t.Optional[bool] = True):

    def get_message(e):
        return e.message.splitlines()[0].replace('Message: ', '')

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except CosmosResourceNotFoundError:
                if raise_not_found:
                    raise ex.NotFoundException() from None
                return None
            except CosmosHttpResponseError as e:
                code = e.status_code
                if code in [400]:
                    raise ex.ValidationException(detail=get_message(e)) from None  # noqa E501
                raise ex.ConfigException(detail=get_message(e)) from None
            except ex.NoSQLException:
                raise
            except Exception as e:
                raise ex.PluginException(detail=e)
        return wrapper
    return decorator


def get_key_kwargs(**kwargs):
    key = dict(kwargs)
    key.pop('abnosql_check_exists', None)
    if len(key) > 2 or len(key) == 0:
        raise ValueError('key length must be 1 or 2')
    keys = list(key.keys())
    kwargs = {
        'item': key[keys[-1]],
        'partition_key': key[keys[0]]
    }
    return kwargs


def strip_cosmos_attrs(item):
    for attr in ['_rid', '_self', '_etag', '_attachments', '_ts']:
        item.pop(attr, None)
    return item


class Table(TableBase):

    def __init__(
        self, pm: PM, name: str, config: t.Optional[dict] = None
    ) -> None:
        self.pm = pm
        self.name = name
        self.set_config(config)
        self.database_client = None
        self.key_attrs = get_key_attrs(self.config)
        self.check_exists = check_exists_enabled(self.config)
        # enabled by default
        self.change_meta = self.config.get(
            'cosmos_change_meta',
            os.environ.get('ABNOSQL_COSMOS_CHANGE_META', 'TRUE') == 'TRUE'
        )

    @cosmos_ex_handler()
    def set_config(self, config: t.Optional[dict]):
        if config is None:
            config = {}
        _config = self.pm.hook.set_config(table=self.name)
        if _config:
            config = t.cast(t.Dict, _config)
        self.config = config

    def _database_client(self):
        _client = self.config.get('database_client', self.database_client)
        if _client is not None:
            return _client
        pc = parse_connstr()
        cf = {}
        if pc is not None:
            cf.update({
                'account': pc.username,
                'database': pc.hostname,
                'credential': (
                    None if (
                        pc.password == 'DefaultAzureCredential'
                        or pc.password == ''
                    )
                    else pc.password
                )
            })
        required = ['endpoint', 'database']
        for attr in ['account', 'credential'] + required:
            val = self.config.get(
                attr, os.environ.get('ABNOSQL_COSMOS_' + attr.upper())
            )
            # override
            if val is not None:
                cf[attr] = val
        if cf.get('endpoint') is None and cf['account'] is not None:
            cf['endpoint'] = 'https://%s.documents.azure.com' % cf['account']
        # use managed identity if no credential supplied
        if cf.get('credential') is None:
            cf['credential'] = DefaultAzureCredential()
        missing = [_ for _ in required if cf[_] is None]
        if len(missing):
            raise ex.ConfigException('missing config: ' + ', '.join(missing))
        self.database_client = CosmosClient(
            url=cf['endpoint'], credential=cf['credential']
        ).get_database_client(cf['database'])
        return self.database_client

    def _container(self, name):
        return self._database_client().get_container_client(name)

    @cosmos_ex_handler()
    def get_item(self, **kwargs) -> t.Optional[t.Dict]:
        _check_exists = dict(**kwargs).pop('abnosql_check_exists', None)
        item = None
        try:
            item = strip_cosmos_attrs(
                self._container(self.name).read_item(
                    **get_key_kwargs(**kwargs)
                )
            )
        except CosmosResourceNotFoundError:
            if _check_exists is False or self.check_exists is False:
                return None
            else:
                raise ex.NotFoundException('item not found')

        _item = self.pm.hook.get_item_post(table=self.name, item=item)
        if _item:
            item = _item
        item = kms_decrypt_item(self.config, item)
        return item

    @cosmos_ex_handler()
    def put_item(
        self,
        item: t.Dict,
        update: t.Optional[bool] = False,
        audit_user: t.Optional[str] = None
    ) -> t.Dict:
        operation = 'update' if update else 'create'
        validate_item(self.config, operation, item)
        item = check_exists(self, operation, item)

        audit_user = audit_user or self.config.get('audit_user')
        if audit_user:
            item = add_audit(item, update or False, audit_user)

        # add change metadata if enabled
        if self.change_meta is True:
            item = add_change_meta(
                item, self.name, 'MODIFY' if update is True else 'INSERT'
            )

        _item = self.pm.hook.put_item_pre(table=self.name, item=item)
        if _item:
            item = _item[0]
        item = kms_encrypt_item(self.config, item)
        # do update
        if update is True:
            validate_key_attrs(self.key_attrs, item)
            key = {k: item.pop(k) for k in self.key_attrs}
            kwargs = {
                'item': key[self.key_attrs[-1]],
                'partition_key': key[self.key_attrs[0]],
                'patch_operations': [
                    {'op': 'add', 'path': f'/{k}', 'value': v}
                    for k, v in item.items()
                ]
            }
            item = self._container(self.name).patch_item(**kwargs)
        # do create/replace
        else:
            item = self._container(self.name).upsert_item(item)
        item = strip_cosmos_attrs(item)
        self.pm.hook.put_item_post(table=self.name, item=item)
        return item

    @cosmos_ex_handler()
    def put_items(
        self,
        items: t.Iterable[t.Dict],
        update: t.Optional[bool] = False,
        audit_user: t.Optional[str] = None
    ):
        # TODO(batch)
        for item in items:
            self.put_item(item, update=update, audit_user=audit_user)
        self.pm.hook.put_items_post(table=self.name, items=items)

    @cosmos_ex_handler()
    def delete_item(self, **kwargs):
        check_exists(self, 'delete', dict(kwargs))

        # if change metadata enabled do update first then delete
        if self.change_meta is True:
            item = add_change_meta(
                dict(**kwargs), self.name, 'REMOVE'
            )
            # don't check if exists when item created
            item['abnosql_check_exists'] = False
            # set update to False because would need key attrs defined if True
            self.put_item(item, update=False)
            # sleep defined number of seconds to allow time between
            # update then delete events.  5 seconds seems to work, less
            # isnt enough time and cosmos doesnt send update event
            sleep_secs = int(os.environ.get(
                'ABNOSQL_COSMOS_CHANGE_META_SLEEPSECS', '5'
            ))
            # disabled if 0
            if sleep_secs > 0:
                time.sleep(sleep_secs)

        self._container(self.name).delete_item(
            **get_key_kwargs(**kwargs)
        )
        self.pm.hook.delete_item_post(table=self.name, key=dict(kwargs))

    @cosmos_ex_handler()
    def query(
        self,
        key: t.Optional[t.Dict[str, t.Any]] = None,
        filters: t.Optional[t.Dict[str, t.Any]] = None,
        limit: t.Optional[int] = None,
        next: t.Optional[str] = None,
        index: t.Optional[str] = None
    ) -> t.Dict[str, t.Any]:
        filters = filters or {}
        key = key or {}
        validate_query_attrs(key, filters)
        parameters = {
            f'@{k}': v
            for k, v in filters.items()
        }
        # cosmos doesnt like hyphens in table names
        table_alias = 'c' if '-' in self.name else self.name
        parameters.update({
            f'@{k}': v
            for k, v in key.items()
        })
        statement = f'SELECT * FROM {table_alias}'
        op = 'WHERE'
        for param in parameters.keys():
            statement += f' {op} {table_alias}.{param[1:]} = {param}'
            op = 'AND'

        resp = self.query_sql(
            statement,
            parameters,
            limit=limit,
            next=next
        )
        resp['items'] = kms_process_query_items(self.config, resp['items'])
        return resp

    @cosmos_ex_handler()
    def query_sql(
        self,
        statement: str,
        parameters: t.Optional[t.Dict[str, t.Any]] = None,
        limit: t.Optional[int] = None,
        next: t.Optional[str] = None
    ) -> t.Dict[str, t.Any]:
        parameters = parameters or {}

        def _get_param(var, val):
            return {'name': var, 'value': val}

        (statement, params) = get_sql_params(
            statement, parameters, _get_param
        )

        kwargs: t.Dict[str, t.Any] = {
            'query': statement,
            'enable_cross_partition_query': True
        }
        if len(params):
            kwargs['parameters'] = params
        # python cosmos SDK doesnt support
        # kwargs['max_item_count'] = limit
        limit = limit or 100
        next = next or '0'

        # Python SDK does not support continuation
        # for cross-partition queries - see limitations https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/cosmos/azure-cosmos  # noqa
        # so add OFFSET and LIMIT if not already present
        if (
            ' OFFSET ' not in statement.upper()
            and ' LIMIT ' not in statement.upper()
        ):
            kwargs['query'] += f' OFFSET {next} LIMIT {limit}'
        logging.debug(f'query_sql() table: {self.name}, kwargs: {kwargs}')
        container = self._container(self.name)
        items = list(container.query_items(**kwargs))
        headers = container.client_connection.last_response_headers
        # continuation = headers.get('x-ms-continuation')
        # total size in 'x-ms-resource-usage' eg ;documentsCount=3
        resource_usage = {
            _.split('=', 1)[0]: _.split('=', 1)[1]
            for _ in headers.get('x-ms-resource-usage', '').split(';')
            if '=' in _
        }
        doc_count = None
        try:
            doc_count = int(resource_usage['documentsCount'])
        except Exception:
            doc_count = None

        for i in range(len(items)):
            items[i] = strip_cosmos_attrs(items[i])
        items = kms_process_query_items(self.config, items)
        _next = None
        try:
            _next = limit + int(next)
        except Exception:
            _next = None
        if doc_count is not None and _next is not None and _next >= doc_count:
            _next = None

        return {
            'items': items,
            'next': str(_next) if _next and len(items) else None
        }
