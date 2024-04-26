import os

from mockfirestore import MockFirestore  # type: ignore
from tests import common as cmn


def config(table='hash_range', extra={}):
    os.environ.update({
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


def test_get_item():
    cmn.test_get_item(config('hash_range'), 'hash_range')
    cmn.test_get_item(config('hash_only'), 'hash_only')


def test_check_exists():
    cmn.test_check_exists(config())


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


def test_query_base():
    cmn.test_query(config())


def test_query_sql():
    cmn.test_query_sql(config())


def test_query_scan():
    cmn.test_query_scan(config())


def test_query_pagination():
    cmn.test_query_pagination(config())
