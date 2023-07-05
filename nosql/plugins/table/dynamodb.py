import functools
from traceback import print_exc
import typing as t

from botocore.exceptions import ClientError  # type: ignore
import pluggy  # type: ignore

import nosql.exceptions as ex
from nosql.plugin import PM
from nosql.table import deserialize
from nosql.table import get_dynamodb_param
from nosql.table import get_dynamodb_query_kwargs
from nosql.table import get_sql_params
from nosql.table import TableBase
from nosql.table import validate_statement

hookimpl = pluggy.HookimplMarker('nosql.table')

try:
    import boto3  # type: ignore
    # import botocore.client
    # import botocore.exceptions
except ImportError:
    MISSING_DEPS = True


def get_key(**kwargs):
    key = dict(kwargs)
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
                    raise ex.NotFoundException(e)
                raise ex.ValidationException(e)
            except Exception as e:
                print_exc()
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
        self.session = self.config.get('session', boto3.session.Session())
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
    def get_item(self, **kwargs) -> t.Dict:
        response = deserialize(self.table.get_item(
            TableName=self.name,
            Key=get_key(**kwargs)
        ), self.config.get('deserializer'))
        item = response.get('Item')
        _item = self.pm.hook.get_item_post(table=self.name, item=item)
        if _item:
            item = _item
        return item

    @dynamodb_ex_handler()
    def put_item(self, item: t.Dict):
        self.table.put_item(Item=item)
        self.pm.hook.put_item_post(table=self.name, item=item)

    @dynamodb_ex_handler()
    def put_items(self, items: t.Iterable[t.Dict]):
        # TODO(batch)
        for item in items:
            self.put_item(item)
        self.pm.hook.put_items_post(table=self.name, items=items)

    @dynamodb_ex_handler()
    def delete_item(self, **kwargs):
        key = get_key(**kwargs)
        self.table.delete_item(Key=key)
        self.pm.hook.delete_item_post(table=self.name, key=key)

    @dynamodb_ex_handler()
    def query(
        self,
        key: t.Dict[str, t.Any],
        filters: t.Optional[t.Dict[str, t.Any]] = None,
        limit: t.Optional[int] = None,
        next: t.Optional[str] = None
    ) -> t.Dict[str, t.Any]:
        kwargs = get_dynamodb_query_kwargs(
            self.name, key, filters
        )
        if next is not None:
            kwargs['ExclusiveStartKey'] = next
        if limit is not None:
            kwargs['Limit'] = limit
        response = self.table.query(**kwargs)
        return {
            'items': deserialize(response.get('Items', [])),
            'next': response.get('LastEvaluatedKey')
        }

    @dynamodb_ex_handler()
    def query_sql(
        self,
        statement: str,
        parameters: t.Optional[t.Dict[str, t.Any]] = None,
        limit: t.Optional[int] = None,
        next: t.Optional[str] = None
    ) -> t.Dict[str, t.Any]:
        validate_statement(statement)

        if parameters is None:
            parameters = {}

        (statement, params) = get_sql_params(
            statement, parameters, get_dynamodb_param, '?'
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

        response = client.execute_statement(**kwargs)
        items = deserialize(response.get('Items', []))
        return {
            'items': items,
            'next': response.get('NextToken')
        }
