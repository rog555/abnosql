import os
from unittest.mock import patch

from mockfirestore import MockFirestore  # type: ignore
import pytest
from tests import common as cmn

from abnosql import exceptions as ex
from abnosql.plugins.table.firestore import Table as FirestoreTable
from abnosql import table

ROOT_DIR = os.path.dirname(os.path.dirname(__file__))
DATA_DIR = os.path.join(ROOT_DIR, 'tests', 'data')


def config(table='hash_range', extra={}):
    os.environ.update({
        'GOOGLE_APPLICATION_CREDENTIALS': os.path.join(
            DATA_DIR, 'google', 'mocked_credentials.json'
        ),
        'ABNOSQL_DB': 'firestore',
        'GOOGLE_CLOUD_PROJECT': 'foo',  # this is to just stop warnings on CLI
        'ABNOSQL_FIRESTORE_DATABASE': 'bar'
    })
    config = {
        'client': MockFirestore(),
        'key_attrs': ['hk', 'rk'] if table == 'hash_range' else ['hk'],
        'batchmode': False  # MockFirestore doesnt support batch yet
    }
    config.update(extra)
    return config


def test_exceptions():
    _config = config()
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'foobar'
    with pytest.raises(ex.ConfigException) as e:
        table('foobar', _config)
    assert 'invalid config' in str(e.value)
    assert e.value.to_problem() == {
        'title': 'invalid config',
        'detail': 'File foobar was not found.',
        'status': 500,
        'type': None
    }


def test_get_item():
    cmn.test_get_item(config('hash_range'), 'hash_range')
    cmn.test_get_item(config('hash_only'), 'hash_only')


# example of patching get_client with MockFirestore
# from mockfirestore import MockFirestore
# from abnosql.plugins.table.firestore import Table as FirestoreTable
@patch.object(FirestoreTable, 'get_client', MockFirestore)
def test_check_exists():
    _config = config()
    _config.pop('client', None)
    cmn.test_check_exists(_config)


def test_validate_item():
    cmn.test_validate_item(config())


def test_put_item():
    cmn.test_put_item(config())


def test_put_item_audit():
    cmn.test_put_item_audit(config())


def test_update_item():
    cmn.test_update_item(config())


def test_put_items():
    cmn.test_put_items(config())


def test_delete_item():
    cmn.test_delete_item(config())


def test_hooks():
    cmn.test_hooks(config())


def test_audit_callback():
    cmn.test_audit_callback(config())


def test_query_base():
    cmn.test_query(config())


def test_query_sql():
    cmn.test_query_sql(config())


def test_query_scan():
    cmn.test_query_scan(config())


def test_query_pagination():
    cmn.test_query_pagination(config())
