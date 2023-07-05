import os

from moto import mock_dynamodb2  # type: ignore

from nosql.mocks import mock_dynamodbx
from tests import common as cmn


def setup_dynamodb():
    os.environ['NOSQL_DB'] = 'dynamodb'


@mock_dynamodb2
def test_get_item():
    setup_dynamodb()
    cmn.test_get_item()


@mock_dynamodb2
def test_put_item():
    setup_dynamodb()
    cmn.test_put_item()


@mock_dynamodb2
def test_put_items():
    setup_dynamodb()
    cmn.test_put_items()


@mock_dynamodb2
def test_delete_item():
    setup_dynamodb()
    cmn.test_delete_item()


@mock_dynamodb2
def test_hooks():
    setup_dynamodb()
    cmn.test_hooks()


@mock_dynamodb2
def test_query():
    setup_dynamodb()
    cmn.test_query()


@mock_dynamodbx
@mock_dynamodb2
def test_query_sql():
    setup_dynamodb()
    cmn.test_query_sql()
