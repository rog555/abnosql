from base64 import b64encode
import os

import responses  # type: ignore

from abnosql.mocks import mock_cosmos
from abnosql.mocks.mock_cosmos import set_keyattrs
from abnosql.plugins.table.memory import clear_tables
from tests import common as cmn


def setup_cosmos():
    clear_tables()
    set_keyattrs({
        'hash_range': ['hk', 'rk'],
        'hash_only': ['hk']
    })
    os.environ['ABNOSQL_DB'] = 'cosmos'
    os.environ['ABNOSQL_COSMOS_ACCOUNT'] = 'foo'
    os.environ['ABNOSQL_COSMOS_CREDENTIAL'] = b64encode(
        'mycredential'.encode('utf-8')
    ).decode()
    os.environ['ABNOSQL_COSMOS_DATABASE'] = 'bar'


@mock_cosmos
@responses.activate
def test_get_item():
    setup_cosmos()
    cmn.test_get_item()


@mock_cosmos
@responses.activate
def test_put_item():
    setup_cosmos()
    cmn.test_put_item()


@mock_cosmos
@responses.activate
def test_put_item_audit():
    setup_cosmos()
    cmn.test_put_item_audit()


@mock_cosmos
@responses.activate
def test_put_items():
    setup_cosmos()
    cmn.test_put_items()


@mock_cosmos
@responses.activate
def test_delete_item():
    setup_cosmos()
    cmn.test_delete_item()


@mock_cosmos
@responses.activate
def test_hooks():
    setup_cosmos()
    cmn.test_hooks()


@mock_cosmos
@responses.activate
def test_query():
    setup_cosmos()
    cmn.test_query()


@mock_cosmos
@responses.activate
def test_query_sql():
    setup_cosmos()
    cmn.test_query_sql()


@mock_cosmos
@responses.activate
def test_query_scan():
    setup_cosmos()
    cmn.test_query_scan()


@mock_cosmos
@responses.activate
def test_query_pagination():
    setup_cosmos()
    cmn.test_query_pagination()
