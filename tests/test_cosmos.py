from base64 import b64encode
import os
import time

from azure.identity import EnvironmentCredential  # type: ignore
import pytest
import responses  # type: ignore

import abnosql.exceptions as ex
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
def test_exceptions():
    setup_cosmos()
    os.environ.pop('ABNOSQL_COSMOS_CREDENTIAL', None)
    vars = ['AZURE_CLIENT_ID', 'AZURE_TENANT_ID', 'AZURE_CLIENT_SECRET']
    orig = {var: os.environ.pop(var, None) for var in vars}
    with pytest.raises(ex.PluginException) as e:
        cmn.test_get_item(config={
            'credential': EnvironmentCredential()
        })
    assert 'plugin exception' in str(e.value)
    assert e.value.to_problem() == {
        'title': 'plugin exception',
        'detail': 'EnvironmentCredential authentication unavailable. Environment variables are not fully configured.\nVisit https://aka.ms/azsdk/python/identity/environmentcredential/troubleshoot to troubleshoot this issue.',  # noqa E501
        'status': 500,
        'type': None
    }

    # restore (prob don't need to do this)
    for var, val in orig.items():
        if val is not None:
            os.environ[var] = val


@mock_cosmos
@responses.activate
def test_get_item():
    setup_cosmos()
    cmn.test_get_item()


@mock_cosmos
@responses.activate
def test_check_exists():
    setup_cosmos()
    cmn.test_check_exists()


@mock_cosmos
@responses.activate
def test_validate_item():
    setup_cosmos()
    cmn.test_validate_item()


@mock_cosmos
@responses.activate
def test_put_item():
    setup_cosmos()
    # test with DefaultAzureCredential
    os.environ.pop('ABNOSQL_COSMOS_CREDENTIAL', None)
    cmn.test_put_item()


@mock_cosmos
@responses.activate
def test_put_item_audit():
    setup_cosmos()
    cmn.test_put_item_audit()


@mock_cosmos
@responses.activate
def test_update_item():
    setup_cosmos()
    cmn.test_update_item()


@mock_cosmos
@responses.activate
def test_put_items():
    setup_cosmos()
    cmn.test_put_items()


@mock_cosmos
@responses.activate
def test_delete_item():
    setup_cosmos()
    start_secs = time.time()
    os.environ['ABNOSQL_COSMOS_CHANGE_META'] = 'TRUE'
    cmn.test_delete_item()
    os.environ.pop('ABNOSQL_COSMOS_CHANGE_META', None)
    # validate that ABNOSQL_COSMOS_CHANGE_META_SLEEPSECS defaulted to 5
    assert time.time() >= start_secs + 5


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
