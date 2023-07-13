from base64 import b64decode
from base64 import b64encode
import functools
import json
import os
import typing as t

from abnosql.crypto import CryptoBase
from abnosql.crypto import get_key_ids
import abnosql.exceptions as ex
from abnosql.plugin import PM


try:
    from azure.identity import DefaultAzureCredential  # type: ignore
    from azure.keyvault.keys.crypto import CryptographyClient  # type: ignore
    from azure.keyvault.keys.crypto import KeyWrapAlgorithm  # type: ignore
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
except ImportError:
    MISSING_DEPS = True


def azure_ex_handler(raise_not_found: t.Optional[bool] = True):

    def get_message(e):
        return e.message.splitlines()[0].replace('Message: ', '')

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            # except CosmosResourceNotFoundError as e:
            #     if raise_not_found:
            #         raise ex.NotFoundException(get_message(e)) from None
            #     return None
            # except CosmosHttpResponseError as e:
            #     code = e.status_code
            #     if code in [400]:
            #         raise ex.ValidationException(get_message(e)) from None
            #     raise ex.ConfigException(get_message(e)) from None
            except Exception as e:
                raise ex.PluginException(e)
        return wrapper
    return decorator


class Crypto(CryptoBase):

    def __init__(
        self, pm: PM, config: t.Optional[dict] = None
    ) -> None:
        self.pm = pm
        self.set_config(config)
        key_ids = self.config.get('key_ids', get_key_ids())
        if not isinstance(key_ids, list) or len(key_ids) == 0:
            raise ex.ConfigException('crypto key_ids required')
        self.key_id = key_ids[0]
        self.crypto_client = CryptographyClient(
            self.key_id, self.config.get(
                'credential', DefaultAzureCredential()
            )
        )

    def set_config(self, config: t.Optional[dict]):
        if config is None:
            config = {}
        _config = self.pm.hook.set_config()
        if _config:
            config = t.cast(t.Dict, _config)
        self.config = config

    @azure_ex_handler()
    def encrypt(self, plaintext: str, context: t.Dict) -> str:
        # azure doesnt have GenerateDataKey equivilent
        # as AWS does, and its encrypt/decrypt APIs
        # are only for use against CMKs not data keys
        # so we must do our own AESGCM key to encrypt/decrypt
        # the plaintext and then use azure to wrap/unwrap
        # this with CMK
        key = AESGCM.generate_key(bit_length=128)
        aad = json.dumps(context).encode()
        aesgcm = AESGCM(key)
        nonce = os.urandom(12)
        # encrypt the key using Azure Key Vault CMK
        enc_key = self.crypto_client.wrap_key(
            KeyWrapAlgorithm.rsa_oaep_256, key
        ).encrypted_key
        del key
        serialized = b64encode(
            json.dumps({
                'ct': b64encode(aesgcm.encrypt(
                    nonce, plaintext.encode(), aad
                )).decode(),
                'nonce': b64encode(nonce).decode(),
                'key': b64encode(enc_key).decode(),
                'aad': b64encode(aad).decode()
            }).encode()
        ).decode()
        return serialized

    @azure_ex_handler()
    def decrypt(self, serialized: str, context: t.Dict) -> str:
        aad = json.dumps(context).encode()
        obj = json.loads(b64decode(serialized))
        for k, v in obj.items():
            obj[k] = b64decode(v)
        # decrypt the key using Azure Key Vault CMK
        key = self.crypto_client.unwrap_key(
            KeyWrapAlgorithm.rsa_oaep_256, obj['key']
        ).key
        aesgcm = AESGCM(key)
        del key
        plaintext = aesgcm.decrypt(obj['nonce'], obj['ct'], aad).decode()
        return plaintext
