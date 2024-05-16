from base64 import b64decode
from base64 import b64encode
import functools
import json
import os
import typing as t

import abnosql.exceptions as ex
from abnosql.kms import get_keys
from abnosql.kms import KmsBase
from abnosql.plugin import PM

try:
    from tink import aead  # type: ignore
    from tink import core  # type: ignore
    from tink.integration import gcpkms  # type: ignore
    from tink import new_keyset_handle  # type: ignore
except ImportError:
    MISSING_DEPS = True


def kms_ex_handler(raise_not_found: t.Optional[bool] = True):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except core.TinkError as e:
                raise ex.ConfigException(detail=str(e))
            except Exception as e:
                raise ex.PluginException(detail=e)
        return wrapper
    return decorator


def mock_remote_aead(*args, **kwargs):
    # used for patching during tests
    # see https://github.com/tink-crypto/tink-py/blob/main/tink/aead/_kms_envelope_aead_test.py  # noqa
    keyset_handle = new_keyset_handle(aead.aead_key_templates.AES256_GCM)
    return keyset_handle.primitive(aead.Aead)


class Kms(KmsBase):

    @kms_ex_handler()
    def __init__(
        self, pm: PM, config: t.Optional[dict] = None
    ) -> None:
        self.pm = pm
        self.config = config or {}
        self.provider = 'gcp'

        self.key_ids = self.config.get('key_ids', get_keys())
        if not isinstance(self.key_ids, list) or len(self.key_ids) == 0:
            raise ex.ConfigException('kms key_ids required')
        self.kek_uri = self.key_ids[0]
        self.credentials = self.config.get(
            'credentials', os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
        )
        # see https://developers.google.com/tink/client-side-encryption
        aead.register()
        self.client = gcpkms.GcpKmsClient(
            self.kek_uri,
            self.credentials
        )
        remote_aead = self.client.get_aead(self.kek_uri)
        self.env_aead = aead.KmsEnvelopeAead(
            aead.aead_key_templates.AES256_GCM, remote_aead
        )

    @kms_ex_handler()
    def encrypt(
        self, plaintext: str, context: t.Dict, key: t.Optional[bytes] = None
    ) -> str:
        ciphertext = self.env_aead.encrypt(
            plaintext.encode(), json.dumps(context).encode()
        )
        return b64encode(ciphertext).decode()

    @kms_ex_handler()
    def decrypt(self, serialized: str, context: t.Dict) -> str:
        plaintext = self.env_aead.decrypt(
            b64decode(serialized), json.dumps(context).encode()
        )
        return plaintext.decode()
