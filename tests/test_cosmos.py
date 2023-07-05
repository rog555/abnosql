from base64 import b64encode
import os

from moto import mock_dynamodb2  # type: ignore
import responses  # type: ignore

from nosql.mocks import mock_cosmos
from tests import common as cmn


def setup_cosmos():
    os.environ['NOSQL_DB'] = 'cosmos'
    os.environ['NOSQL_COSMOS_ACCOUNT'] = 'foo'
    os.environ['NOSQL_COSMOS_CREDENTIAL'] = b64encode(
        'mycredential'.encode('utf-8')
    ).decode()
    os.environ['NOSQL_COSMOS_DATABASE'] = 'bar'


@mock_dynamodb2
@mock_cosmos
@responses.activate
def test_get_item():
    setup_cosmos()
    cmn.test_get_item()


@mock_cosmos
@mock_dynamodb2
@responses.activate
def test_put_item():
    setup_cosmos()
    cmn.test_put_item()


@mock_cosmos
@mock_dynamodb2
@responses.activate
def test_put_items():
    setup_cosmos()
    cmn.test_put_items()


@mock_cosmos
@mock_dynamodb2
@responses.activate
def test_delete_item():
    setup_cosmos()
    cmn.test_delete_item()


@mock_cosmos
@mock_dynamodb2
@responses.activate
def test_hooks():
    setup_cosmos()
    cmn.test_hooks()


@mock_cosmos
@mock_dynamodb2
@responses.activate
def test_query():
    setup_cosmos()
    cmn.test_query()


@mock_cosmos
@mock_dynamodb2
@responses.activate
def test_query_sql():
    setup_cosmos()
    cmn.test_query_sql()
