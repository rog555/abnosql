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
                    raise ex.NotFoundException(detail=e.message) from None
                return None
            except Exception as e:
                raise ex.PluginException(detail=e)
        return wrapper
    return decorator


class Kms(KmsBase):

    def __init__(
        self, pm: PM, config: t.Optional[dict] = None
    ) -> None:
        self.pm = pm
        self.config = config or {}
        self.provider = 'azure'
        key_ids = self.config.get('key_ids', get_keys())
        if not isinstance(key_ids, list) or len(key_ids) == 0:
            raise ex.ConfigException('kms key_ids required')
        self.key_id = key_ids[0]
        self.crypto_client = CryptographyClient(
            self.key_id, self.config.get(
                'credential', DefaultAzureCredential()
            )
        )
        self.pack_bytes_maxlen = self.config.get(
            'pack_bytes_maxlen', 10000
        )

    @kms_ex_handler()
    def encrypt(
        self, plaintext: str, context: t.Dict, key: t.Optional[bytes] = None
    ) -> str:
        # azure doesnt have GenerateDataKey equivilent
        # as AWS does, and its encrypt/decrypt APIs
        # are only for use against CMKs not data keys
        # so generate own AESGCM DEK to encrypt/decrypt
        # the plaintext and then use azure to wrap/unwrap this with CMK.
        # This follows similar pattern to aws-encryption-sdk and google tink
        # and the wrapped/encrypted AES key lives with the data
        # see https://developers.google.com/tink/client-side-encryption
        # https://docs.aws.amazon.com/encryption-sdk/latest/developer-guide/concepts.html  # noqa
        context = dict(sorted(context.items()))
        aad = json.dumps(context).encode()

        # 1) generate random Data Encryption Key (DEK)
        # 256-bit AES-GCM key with 96-bit nonce
        nonce = os.urandom(96)
        dek = key or AESGCM.generate_key(bit_length=256)
        dek_aesgcm = AESGCM(dek)

        # 2) The DEK is encrypted by a Key Encryption Key (KEK)
        # that is stored in a cloud KMS (Azure Key Vault CMK)
        enc_dek = self.crypto_client.wrap_key(
            KeyWrapAlgorithm.rsa_oaep_256, dek
        ).encrypted_key
        del dek  # delete unencrypted DEK from memory asap

        # 3) Data is encrypted using the DEK by the client.
        ct = dek_aesgcm.encrypt(nonce, plaintext.encode(), aad)
        del dek_aesgcm

        # 4) Concatenates the KEK-encrypted encryption DEK with the encrypted
        # data (byte packing is what aws-encryption-sdk and google tink do)
        serialized = b64encode(
            pack_bytes([ct, nonce, enc_dek], self.pack_bytes_maxlen)
        ).decode()
        return serialized

    @kms_ex_handler()
    def decrypt(self, serialized: str, context: t.Dict) -> str:
        context = dict(sorted(context.items()))
        aad = json.dumps(context).encode()

        # 1) Extracts the KEK-encrypted DEK key.
        unpacked = unpack_bytes(b64decode(serialized.encode()))
        if len(unpacked) != 3:
            raise ValueError('invalid serialization')
        (ct, nonce, enc_dek) = unpacked

        # 2) Makes a request to your KMS to decrypt the KEK-encrypted DEK.
        # decrypt the key using Azure Key Vault CMK
        dek = self.crypto_client.unwrap_key(
            KeyWrapAlgorithm.rsa_oaep_256, enc_dek
        ).key

        # 3) Decrypts the ciphertext locally using the DEK.
        dek_aesgcm = AESGCM(dek)
        del dek
        plaintext = dek_aesgcm.decrypt(nonce, ct, aad).decode()
        return plaintext
