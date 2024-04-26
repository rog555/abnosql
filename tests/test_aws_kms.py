from base64 import b64decode
import os

import boto3  # type: ignore
from boto3.dynamodb.types import Decimal  # type: ignore
from moto import mock_aws  # type: ignore

from abnosql.mocks import mock_dynamodbx
from tests import common as cmn


def create_table(name, rk=True):
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    key_schema = [
        {'AttributeName': 'hk', 'KeyType': 'HASH'}
    ]
    attr_defs = [
        {'AttributeName': 'hk', 'AttributeType': 'S'}
    ]
    if rk is True:
        key_schema.append({'AttributeName': 'rk', 'KeyType': 'RANGE'})
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


def get_table(name):
    return boto3.resource('dynamodb', region_name='us-east-1').Table(name)


def setup_dynamodb():
    os.environ['ABNOSQL_DB'] = 'dynamodb'
    os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'
    create_table('hash_range', True)
    kms = boto3.client('kms')
    resp = kms.create_key(
        Policy='test kms'
    )
    return {
        'kms': {
            'key_ids': [resp['KeyMetadata']['Arn']],
            'key_attrs': ['hk', 'rk'],
            'attrs': ['obj', 'str']
        }
    }


@mock_aws
def test_get_put_item():
    config = setup_dynamodb()
    cmn.test_get_item(config, 'hash_range')

    # check its encrypted
    resp = get_table('hash_range').get_item(Key={'hk': '1', 'rk': 'a'})
    item = resp['Item']
    assert b'aws-crypto-public-key' in b64decode(item['obj'])
    assert b'aws-crypto-public-key' in b64decode(item['str'])
    assert item['num'] == Decimal('5')


@mock_aws
def test_query():
    config = setup_dynamodb()
    resp = cmn.test_query(config, return_response=True)
    for item in resp['items']:
        assert 'str' not in item and 'obj' not in item
        assert item['num'] == 5


@mock_dynamodbx
@mock_aws
def test_query_sql():
    config = setup_dynamodb()
    resp = cmn.test_query_sql(config, return_response=True)
    for item in resp['items']:
        assert 'str' not in item and 'obj' not in item
        assert item['num'] == 5
