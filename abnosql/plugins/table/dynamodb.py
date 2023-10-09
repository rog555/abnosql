from base64 import b64decode
from base64 import b64encode
from datetime import datetime
import functools
import json
import logging
import os
import typing as t

import pluggy  # type: ignore

import abnosql.exceptions as ex
from abnosql.plugin import PM
from abnosql.table import add_audit
from abnosql.table import check_exists
from abnosql.table import check_exists_enabled
from abnosql.table import get_key_attrs
from abnosql.table import get_sql_params
from abnosql.table import kms_decrypt_item
from abnosql.table import kms_encrypt_item
from abnosql.table import kms_process_query_items
from abnosql.table import TableBase
from abnosql.table import validate_item
from abnosql.table import validate_key_attrs
from abnosql.table import validate_query_attrs

hookimpl = pluggy.HookimplMarker('abnosql.table')

AWS_DEFAULT_REGION = os.environ.get('AWS_DEFAULT_REGION', 'us-east-1')

try:
    import boto3  # type: ignore
    from boto3.dynamodb.types import Binary  # type: ignore
    from boto3.dynamodb.types import Decimal  # type: ignore
    from botocore.exceptions import ClientError  # type: ignore
    from botocore.exceptions import NoCredentialsError  # type: ignore
    from dynamodb_json import json_util  # type: ignore
except ImportError:
    MISSING_DEPS = True


# http://stackoverflow.com/questions/11875770/how-to-overcome-datetime-datetime-not-json-serializable-in-python  # noqa
# see https://github.com/Alonreznik/dynamodb-json/blob/master/dynamodb_json/json_util.py  # noqa
def json_serial(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj) if obj != obj.to_integral_value() else int(obj)
    if isinstance(obj, Binary):
        return obj.value
    if isinstance(obj, set):
        return list(obj)
    raise TypeError('type not serializable')


def deserialize(obj, deserializer=None):
    if deserializer is None:
        deserializer = json_serial
    elif callable(deserializer):
        return deserializer(obj)
    return json.loads(json.dumps(obj, default=deserializer))


def get_key(**kwargs):
    key = dict(kwargs)
    key.pop('abnosql_check_exists', None)
    if len(key) > 2 or len(key) == 0:
        raise ValueError('key length must be 1 or 2')
    return key


def dynamodb_ex_handler(raise_not_found: t.Optional[bool] = True):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except ClientError as e:
                code = e.response['Error']['Code']
                if raise_not_found and code in ['ResourceNotFoundException']:
                    raise ex.NotFoundException() from None
                elif code == 'UnrecognizedClientException':
                    raise ex.ConfigException(detail=e) from None
                raise ex.ValidationException(detail=e) from None
            except NoCredentialsError as e:
                raise ex.ConfigException(detail=e) from None
            except ex.NoSQLException:
                raise
            except Exception as e:
                raise ex.PluginException(detail=e)
        return wrapper
    return decorator


def serialize_dynamodb_type(var, val):
    _type = (
        'N' if isinstance(val, float) or isinstance(val, int)
        else 'M' if isinstance(val, float)
        else 'L' if isinstance(val, list)
        else 'B' if isinstance(val, bytes)
        else 'NULL' if val is None
        else 'BOOL' if isinstance(val, bool)
        else 'S'
    )
    return {_type: str(val)}


def get_dynamodb_kwargs(
    name: str,
    key: t.Optional[t.Dict[str, t.Any]] = None,
    filters: t.Optional[t.Dict[str, t.Any]] = None,
    index: t.Optional[str] = None
) -> t.Dict:
    key = key or {}
    if len(key) > 2:
        raise ValueError('key length must be 1 or 2')
    filters = filters or {}
    validate_query_attrs(key, filters)

    _values = {}
    if len(key):
        _values = {
            f':{k}': v
            for k, v in key.items()
        }
    _names = {}
    for k, v in filters.items():
        _names[f'#{k}'] = k
        _values[f':{k}'] = v

    kwargs: t.Dict[str, t.Any] = {
        'TableName': name,
        'Select': 'ALL_ATTRIBUTES'
    }
    if index is not None:
        kwargs['IndexName'] = index
    if len(key):
        kwargs['KeyConditionExpression'] = ' AND '.join([
            f'{k} = :{k}' for k in key.keys()
        ])
    if len(_values):
        kwargs['ExpressionAttributeValues'] = _values
    if len(_names):
        kwargs['ExpressionAttributeNames'] = _names
    if len(filters):
        kwargs['FilterExpression'] = ' AND '.join([
            f'#{k} = :{k}' for k in filters.keys()
        ])
    return kwargs


class Table(TableBase):

    def __init__(
        self, pm: PM, name: str, config: t.Optional[dict] = None
    ) -> None:
        self.pm = pm
        self.name = name
        self.set_config(config)
        self.session = self.config.get(
            'session', boto3.session.Session(
                region_name=AWS_DEFAULT_REGION
            )
        )
        self.key_attrs = get_key_attrs(self.config)
        self.check_exists = check_exists_enabled(self.config)
        self.table = self.session.resource('dynamodb').Table(name)

    @dynamodb_ex_handler()
    def set_config(self, config: t.Optional[dict]):
        if config is None:
            config = {}
        _config = self.pm.hook.set_config(table=self.name)
        if _config:
            config = t.cast(t.Dict, _config)
        self.config = config

    @dynamodb_ex_handler()
    def get_item(self, **kwargs) -> t.Optional[t.Dict]:
        response = deserialize(self.table.get_item(
            TableName=self.name,
            Key=get_key(**kwargs)
        ), self.config.get('deserializer'))
        _check_exists = dict(**kwargs).pop('abnosql_check_exists', None)
        item = response.get('Item')
        _item = self.pm.hook.get_item_post(table=self.name, item=item)
        if _item:
            item = _item
        item = kms_decrypt_item(self.config, item)
        if _check_exists is not False:
            check_exists(self, 'get', item)
        return item

    @dynamodb_ex_handler()
    def put_item(
        self, item:
        t.Dict,
        update: t.Optional[bool] = False,
        audit_user: t.Optional[str] = None
    ) -> t.Dict:
        operation = 'update' if update else 'create'
        validate_item(self.config, operation, item)
        item = check_exists(self, operation, item)

        audit_user = audit_user or self.config.get('audit_user')
        if audit_user:
            item = add_audit(item, update or False, audit_user)
        _item = self.pm.hook.put_item_pre(table=self.name, item=item)
        if _item:
            item = _item[0]
        item = kms_encrypt_item(self.config, item)

        # do update
        if update is True:
            validate_key_attrs(self.key_attrs, item)
            kwargs = {
                'Key': {k: item.pop(k) for k in self.key_attrs},
                'ReturnValues': 'ALL_NEW'
            }
            exp = []
            vals = {}
            aliases = {}
            for k, v in sorted(item.items()):
                if isinstance(v, str) and v == '':
                    v = None
                aliases['#%s' % k] = k
                exp.append('#%s = :%s' % (k, k))
                vals[':%s' % k] = v
            kwargs['UpdateExpression'] = 'set %s' % ', '.join(exp)
            kwargs['ExpressionAttributeNames'] = aliases
            kwargs['ExpressionAttributeValues'] = vals
            response = self.table.update_item(**kwargs)
            item.update(response.get('Attributes'))

        # do create/replace
        else:
            self.table.put_item(Item=item)

        self.pm.hook.put_item_post(table=self.name, item=item)
        return item

    @dynamodb_ex_handler()
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

    @dynamodb_ex_handler()
    def delete_item(self, **kwargs):
        check_exists(self, 'delete', dict(kwargs))
        key = get_key(**kwargs)
        self.table.delete_item(Key=key)
        self.pm.hook.delete_item_post(table=self.name, key=key)

    @dynamodb_ex_handler()
    def query(
        self,
        key: t.Optional[t.Dict[str, t.Any]] = None,
        filters: t.Optional[t.Dict[str, t.Any]] = None,
        limit: t.Optional[int] = None,
        next: t.Optional[str] = None,
        index: t.Optional[str] = None
    ) -> t.Dict[str, t.Any]:
        kwargs = get_dynamodb_kwargs(
            self.name, key, filters=filters, index=index
        )
        if next is not None:
            kwargs['ExclusiveStartKey'] = json.loads(b64decode(next).decode())
        if limit is not None:
            kwargs['Limit'] = limit
        response = None
        if key is not None:
            logging.debug(f'query() table: {self.name}, query kwargs: {kwargs}')
            response = self.table.query(**kwargs)
        else:
            logging.debug(f'query() table: {self.name}, scan kwargs: {kwargs}')
            response = self.table.scan(**kwargs)
        items = response.get('Items', [])
        items = kms_process_query_items(self.config, items)
        last = response.get('LastEvaluatedKey')
        if last is not None:
            last = b64encode(json.dumps(last).encode()).decode()
        return {
            'items': deserialize(items, self.config.get('deserializer')),
            'next': last
        }

    @dynamodb_ex_handler()
    def query_sql(
        self,
        statement: str,
        parameters: t.Optional[t.Dict[str, t.Any]] = None,
        limit: t.Optional[int] = None,
        next: t.Optional[str] = None
    ) -> t.Dict[str, t.Any]:
        parameters = parameters or {}
        (statement, params) = get_sql_params(
            statement, parameters, serialize_dynamodb_type, '?'
        )
        client = self.session.client('dynamodb')
        kwargs: t.Dict[str, t.Any] = {
            'Statement': statement
        }
        if next is not None:
            kwargs['NextToken'] = next
        if limit is not None:
            kwargs['Limit'] = limit
        if len(params):
            kwargs['Parameters'] = params

        logging.debug(f'query_sql() table: {self.name}, kwargs: {kwargs}')
        response = client.execute_statement(**kwargs)
        items = []
        _items = response.get('Items', [])
        _items = kms_process_query_items(self.config, _items)
        for item in _items:
            items.append(json_util.loads(json.dumps(item)))

        return {
            'items': items,
            'next': response.get('NextToken')
        }
