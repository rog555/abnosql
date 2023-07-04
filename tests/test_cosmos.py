from base64 import b64encode
import os
import sqlite3

from moto import mock_dynamodb2  # type: ignore
import responses  # type: ignore

from nosql.mocks import mock_cosmos
from nosql.mocks import mock_dynamodbx
from tests import common as cmn


TABLE_KEYS = {'hash_range': ['hk', 'rk'], 'hash_only': ['hk']}
DB = sqlite3.connect(':memory:')


def setup_cosmos():
    os.environ['NOSQL_DB'] = 'cosmos'
    os.environ['NOSQL_COSMOS_ACCOUNT'] = 'foo'
    os.environ['NOSQL_COSMOS_CREDENTIAL'] = b64encode(
        'mycredential'.encode('utf-8')
    ).decode()
    os.environ['NOSQL_COSMOS_DATABASE'] = 'bar'


@mock_cosmos(TABLE_KEYS)
@mock_dynamodb2
@responses.activate
def test_get_item():
    setup_cosmos()
    cmn.test_get_item()


@mock_cosmos(TABLE_KEYS)
@mock_dynamodb2
@responses.activate
def test_put_item():
    setup_cosmos()
    cmn.test_put_item()


@mock_cosmos(TABLE_KEYS)
@mock_dynamodb2
@responses.activate
def test_put_items():
    setup_cosmos()
    cmn.test_put_items()


@mock_cosmos(TABLE_KEYS)
@mock_dynamodb2
@responses.activate
def test_delete_item():
    setup_cosmos()
    cmn.test_delete_item()


@mock_cosmos(TABLE_KEYS, db=DB)
@mock_dynamodbx(db=DB)
@responses.activate
def test_query():
    setup_cosmos()
    cmn.test_query(db=DB)


@mock_cosmos(TABLE_KEYS, db=DB)
@mock_dynamodbx(db=DB)
@responses.activate
def test_query_sql():
    setup_cosmos()
    cmn.test_query_sql(db=DB)
