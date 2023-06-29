import os

import boto3
from moto import mock_dynamodb2


from nosql import table


def _item(hk, rk=None):
    item = {
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
        item['rk'] = rk
    return item


def create_table(name, hks=None, rks=None):
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    key_schema = [
        {'AttributeName': 'hk', 'KeyType': 'HASH'}
    ]
    attr_defs = [
        {'AttributeName': 'hk', 'AttributeType': 'S'}
    ]
    if rks is not None:
        key_schema.append({'AttributeName': 'rk', 'KeyType': 'HASH'})
        attr_defs.append({'AttributeName': 'rk', 'AttributeType': 'S'})                   
    params = {
        'TableName': name,
        'KeySchema': key_schema,
        'AttributeDefinitions': attr_defs,
        'ProvisionedThroughput': {
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    }
    dynamodb.create_table(**params)
    _table = dynamodb.Table('foo')
    for hk in hks:
        if rks:
            for rk in rks or []:
                _table.put_item(Item=_item(hk, rk))
        else:
            _table.put_item(Item=_item(hk))


@mock_dynamodb2
def test_get_item():
    os.environ['NOSQL_DB'] = 'dynamodb'
    create_table('foo', ['1', '2'])
    tb = table('foo')
    assert tb.get_item(hk='1', rk='1') == _item('1', '1')
