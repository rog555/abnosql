import os

from moto import mock_dynamodb2  # type: ignore

from tests import common as cmn
from tests.mock_boto3 import mock_boto3


def setup():
    os.environ['NOSQL_DB'] = 'dynamodb'


@mock_dynamodb2
def test_get_item():
    setup()
    cmn.test_get_item()


@mock_dynamodb2
def test_get_item_hook():
    setup()
    cmn.test_get_item_hook()


@mock_dynamodb2
def test_put_item():
    setup()
    cmn.test_put_item()


@mock_dynamodb2
def test_put_items():
    setup()
    cmn.test_put_items()


@mock_dynamodb2
def test_delete_item():
    setup()
    cmn.test_delete_item()


@mock_boto3
def test_query():
    setup()
    cmn.test_query()
