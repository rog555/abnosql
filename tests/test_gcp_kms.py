from base64 import b64decode
import os
from unittest.mock import patch

from mockfirestore import MockFirestore  # type: ignore
import pytest
from tink.integration import gcpkms

from tests import common as cmn

from abnosql import exceptions as ex
from abnosql.kms import kms
from abnosql.plugins.kms.gcp import mock_remote_aead

ROOT_DIR = os.path.dirname(os.path.dirname(__file__))
DATA_DIR = os.path.join(ROOT_DIR, 'tests', 'data')
KEY_URI = 'gcp-kms://projects/p1/locations/global/keyRings/kr1/cryptoKeys/ck1'


def setup_gcp():
    os.environ.update({
        'ABNOSQL_DB': 'firestore',
        'ABNOSQL_FIRESTORE_DATABASE': 'bar',
        'GOOGLE_APPLICATION_CREDENTIALS': os.path.join(
            DATA_DIR, 'google', 'mocked_credentials.json'
        ),
        'GOOGLE_CLOUD_PROJECT': 'foo'  # this is to just stop warnings on CLI
    })
    config = {
        'client': MockFirestore(),
        'key_attrs': ['hk', 'rk'],
        'kms': {
            'key_ids': [KEY_URI],
            'key_attrs': ['hk', 'rk'],
            'attrs': ['obj', 'str'],
        },
        'batchmode': False  # MockFirestore doesnt support batch yet
    }
    return config


@patch.object(gcpkms.GcpKmsClient, 'get_aead', mock_remote_aead)
def test_get_put_item():
    config = setup_gcp()
    tb = cmn.test_get_item(config, 'hash_range')

    # check its encrypted
    item = tb.table.document('1:a').get().to_dict()

    assert b64decode(item['obj']).startswith(b'\x00\x00')
    assert b64decode(item['str']).startswith(b'\x00\x00')
    assert item['num'] == 5


@patch.object(gcpkms.GcpKmsClient, 'get_aead', mock_remote_aead)
def test_query():
    config = setup_gcp()
    resp = cmn.test_query(config, return_response=True)
    for item in resp['items']:
        assert 'str' not in item and 'obj' not in item
        assert item['num'] == 5


@patch.object(gcpkms.GcpKmsClient, 'get_aead', mock_remote_aead)
def test_query_sql():
    config = setup_gcp()
    resp = cmn.test_query_sql(config, return_response=True)
    for item in resp['items']:
        assert 'str' not in item and 'obj' not in item
        assert item['num'] == 5


@patch.object(gcpkms.GcpKmsClient, 'get_aead', mock_remote_aead)
def test_invalid_context():
    config = setup_gcp()
    tb = cmn.test_get_item(config, 'hash_range')
    item = tb.table.document('1:a').get().to_dict()
    _kms = kms(config['kms'], 'gcp')
    with pytest.raises(ex.ConfigException) as e:
        _kms.decrypt(item['obj'], {'hk': 'invalid', 'rk': 'invalid'})

    assert 'invalid config' in str(e.value)
    assert e.value.to_problem() == {
        'title': 'invalid config',
        'detail': 'Decryption failed.',
        'status': 500,
        'type': None
    }
