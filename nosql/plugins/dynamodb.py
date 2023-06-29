from datetime import datetime
import json
import typing as ty

from boto3.dynamodb.types import Binary
from boto3.dynamodb.types import Decimal

try:
    import boto3
    # import botocore.client
    # import botocore.exceptions
except ImportError:
    MISSING_DEPS = True


# http://stackoverflow.com/questions/11875770/
# how-to-overcome-datetime-datetime-not-json-serializable-in-python
# see https://github.com/Alonreznik/dynamodb-json/blob/
# master/dynamodb_json/json_util.py
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
    if len(key) > 2 or len(key) == 0:
        raise ValueError('key lenght must be 1 or 2')
    return key


class Table():

    def __init__(self, name: str, config: ty.Optional[dict] = None) -> None:
        self.table_name = name
        self.table = boto3.resource('dynamodb').Table(name)
        if config is None:
            config = {}
        self.config = config

    def get_item(self, **kwargs) -> ty.Dict:
        response = deserialize(self.table.get_item(
            TableName=self.table_name,
            Key=get_key(**kwargs)
        ), self.config.get('deserializer'))
        return response.get('Item')

    def put_item(self, item: ty.Dict) -> bool:
        self.table.put_item(
            Item=item
        )
        return True

    def put_items(self, items: ty.Iterable[ty.Dict]) -> bool:
        # TODO(batch)
        for item in items:
            self.put_item(item)
        return True

    def delete_item(self, **kwargs) -> bool:
        self.table.delete_item(
            Key=get_key(**kwargs)
        )
        return True

    def query(self, query: str) -> ty.Dict:
        print('dynamodb.query()')
