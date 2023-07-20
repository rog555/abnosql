from base64 import b64decode
from base64 import b64encode
import os

import responses  # type: ignore

from abnosql.mocks import mock_azure_kms
from abnosql.mocks.mock_azure_kms import AESGCM_KEY
from abnosql.mocks import mock_cosmos
from abnosql.mocks.mock_cosmos import set_keyattrs
from abnosql.plugins.table.memory import clear_tables
from abnosql import table
from tests import common as cmn


KEY_ID = (
    # random guid
    'https://foo.vault.azure.net/keys/bar/45e36a1024a04062bd489db0d9004d09'
)


def setup_cosmos():
    clear_tables()
    set_keyattrs({
        'hash_range': ['hk', 'rk']
    })
    cred = b64encode('mycredential'.encode('utf-8')).decode()
    # random guids
    os.environ['ABNOSQL_DB'] = f'cosmos://foo:{cred}@bar'
    os.environ['AZURE_CLIENT_ID'] = '64d6d18d-b5ec-42d8-baeb-4255dd360ea2'
    os.environ['AZURE_TENANT_ID'] = 'ddb6e3bf-68ca-4216-a84f-d3a4b05c8314'
    os.environ['AZURE_CLIENT_SECRET'] = 'foobar12345'
    os.environ['ABNOSQL_KMS_KEYS'] = KEY_ID
    return {
        'kms': {
            'key_attrs': ['hk', 'rk'],
            'attrs': ['obj', 'str'],
            'key_bytes': b64decode(AESGCM_KEY)
        }
    }


@mock_azure_kms
@mock_cosmos
@responses.activate
def test_get_put_item():
    config = setup_cosmos()
    cmn.test_get_item(config, ['hash_range'])

    # check its encrypted
    tb = table('hash_range', database='memory')
    item = tb.get_item(hk='1', rk='a')

    assert item['obj'].startswith('AAABp')
    assert item['str'].startswith('AAABf')
    assert item['num'] == 5


@mock_azure_kms
@mock_cosmos
@responses.activate
def test_query():
    config = setup_cosmos()
    resp = cmn.test_query(config, return_response=True)
    for item in resp['items']:
        assert 'str' not in item and 'obj' not in item
        assert item['num'] == 5


@mock_azure_kms
@mock_cosmos
@responses.activate
def test_query_sql():
    config = setup_cosmos()
    resp = cmn.test_query_sql(config, return_response=True)
    for item in resp['items']:
        assert 'str' not in item and 'obj' not in item
        assert item['num'] == 5
