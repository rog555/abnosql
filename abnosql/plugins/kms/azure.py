from base64 import b64decode
from base64 import b64encode
import functools
import json
import os
import typing as t

import abnosql.exceptions as ex
from abnosql.kms import get_keys
from abnosql.kms import KmsBase
from abnosql.kms import pack_bytes
from abnosql.kms import unpack_bytes
from abnosql.plugin import PM


try:
    import azure.core.exceptions as azex  # type: ignore
    from azure.identity import DefaultAzureCredential  # type: ignore
    from azure.keyvault.keys.crypto import CryptographyClient  # type: ignore
    from azure.keyvault.keys.crypto import KeyWrapAlgorithm  # type: ignore
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
except ImportError:
    MISSING_DEPS = True


def kms_ex_handler(raise_not_found: t.Optional[bool] = True):

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except azex.ResourceNotFoundError as e:
                if raise_not_found:
                    raise ex.NotFoundException(e.message) from None
                return None
            except Exception as e:
                raise ex.PluginException(e)
        return wrapper
    return decorator


class Kms(KmsBase):

    def __init__(
        self, pm: PM, config: t.Optional[dict] = None
    ) -> None:
        self.pm = pm
        self.config = config or {}
        key_ids = self.config.get('key_ids', get_keys())
        if not isinstance(key_ids, list) or len(key_ids) == 0:
            raise ex.ConfigException('kms key_ids required')
        self.key_id = key_ids[0]
        self.crypto_client = CryptographyClient(
            self.key_id, self.config.get(
                'credential', DefaultAzureCredential()
            )
        )

    @kms_ex_handler()
    def encrypt(
        self, plaintext: str, context: t.Dict, key: t.Optional[bytes] = None
    ) -> str:
        # azure doesnt have GenerateDataKey equivilent
        # as AWS does, and its encrypt/decrypt APIs
        # are only for use against CMKs not data keys
        # so we must do our own AESGCM key to encrypt/decrypt
        # the plaintext and then use azure to wrap/unwrap
        # this with CMK.
        # This follows similar pattern to aws-encryption-sdk
        # and the wrapped/encrypted AES key lives with the data
        context = dict(sorted(context.items()))
        aad = json.dumps(context).encode()
        # 256-bit AES-GCM key with 96-bit nonce
        nonce = os.urandom(96)
        key = key or AESGCM.generate_key(bit_length=256)
        aesgcm = AESGCM(key)
        # encrypt the key using Azure Key Vault CMK
        enc_key = self.crypto_client.wrap_key(
            KeyWrapAlgorithm.rsa_oaep_256, key
        ).encrypted_key
        # delete unencrypted key from memory asap
        del key
        # encrypt
        ct = aesgcm.encrypt(nonce, plaintext.encode(), aad)
        del aesgcm
        # byte packing is smaller than json and what aws-encryption-sdk does
        serialized = b64encode(
            pack_bytes([ct, nonce, enc_key], 10000)
        ).decode()
        return serialized

    @kms_ex_handler()
    def decrypt(self, serialized: str, context: t.Dict) -> str:
        context = dict(sorted(context.items()))
        aad = json.dumps(context).encode()
        unpacked = unpack_bytes(b64decode(serialized.encode()))
        if len(unpacked) != 3:
            raise ValueError('invalid serialization')
        (ct, nonce, enc_key) = unpacked
        # decrypt the key using Azure Key Vault CMK
        key = self.crypto_client.unwrap_key(
            KeyWrapAlgorithm.rsa_oaep_256, enc_key  # obj['key']
        ).key
        aesgcm = AESGCM(key)
        del key
        plaintext = aesgcm.decrypt(nonce, ct, aad).decode()
        # plaintext = aesgcm.decrypt(obj['nonce'], obj['ct'], aad).decode()
        return plaintext
