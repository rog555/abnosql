from base64 import b64decode
from base64 import b64encode
import functools
import os
import typing as t

import abnosql.exceptions as ex
from abnosql.kms import get_keys
from abnosql.kms import KmsBase
from abnosql.plugin import PM


try:
    import aws_encryption_sdk  # type: ignore
    from aws_encryption_sdk import CommitmentPolicy  # type: ignore
    from botocore.exceptions import ClientError  # type: ignore
    from botocore.exceptions import NoCredentialsError  # type: ignore
    from botocore.session import Session  # type: ignore
except ImportError:
    MISSING_DEPS = True


AWS_DEFAULT_REGION = os.environ.get('AWS_DEFAULT_REGION', 'us-east-1')


def kms_ex_handler(raise_not_found: t.Optional[bool] = True):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except ClientError as e:
                code = e.response['Error']['Code']
                if raise_not_found and code in ['ResourceNotFoundException']:
                    raise ex.NotFoundException(e) from None
                elif code == 'UnrecognizedClientException':
                    raise ex.ConfigException(e) from None
                raise ex.ValidationException(e) from None
            except NoCredentialsError as e:
                raise ex.ConfigException(e) from None
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
        self.session = self.config.get(
            'session', Session()
        )
        self.key_ids = self.config.get('key_ids', get_keys())
        if not isinstance(self.key_ids, list) or len(self.key_ids) == 0:
            raise ex.ConfigException('kms key_ids required')
        self.client = aws_encryption_sdk.EncryptionSDKClient(
            commitment_policy=CommitmentPolicy.REQUIRE_ENCRYPT_REQUIRE_DECRYPT
        )
        self.mkp = aws_encryption_sdk.StrictAwsKmsMasterKeyProvider(
            key_ids=self.key_ids,
            botocore_session=self.session
        )

    @kms_ex_handler()
    def encrypt(
        self, plaintext: str, context: t.Dict, key: t.Optional[bytes] = None
    ) -> str:
        # not using aws dynamodb encryption sdk in case in future
        # we want to use another aws database (eg postgres)
        ciphertext, _ = self.client.encrypt(
            source=plaintext,
            key_provider=self.mkp,
            encryption_context=context
        )
        return b64encode(ciphertext).decode()

    @kms_ex_handler()
    def decrypt(self, serialized: str, context: t.Dict) -> str:
        plaintext, header = self.client.decrypt(
            source=b64decode(serialized),
            key_provider=self.mkp
        )
        for k, v in header.encryption_context.items():
            if k == 'aws-crypto-public-key':
                continue
            if v != context.get(k):
                raise ex.ValidationException(
                    'context mismatch'
                )
        return plaintext.decode()
