from abc import ABCMeta  # type: ignore
from abc import abstractmethod
import os
import struct
import typing as t

import pluggy  # type: ignore

import abnosql.exceptions as ex
from abnosql import plugin

hookspec = pluggy.HookspecMarker('abnosql.kms')


class KmsBase(metaclass=ABCMeta):
    @abstractmethod
    def __init__(
        self, pm: plugin.PM, config: t.Optional[dict] = None
    ) -> None:
        """Instantiate kms object

        Args:

            pm: pluggy plugin manager
            config: optional config dict dict
        """
        pass

    @abstractmethod
    def encrypt(
        self, plaintext: str, context: t.Dict, key: t.Optional[bytes] = None
    ) -> str:
        """encrypt plaintext string

        Args:

            value: plaintext string
            context: encryption context / AAD dictionary

        Returns:

            serialized encrypted string

        """
        pass

    @abstractmethod
    def decrypt(self, serialized: str, context: t.Dict) -> str:
        """decrypt serialized encrypted string

        Args:

            serialized: serialized encrypted string
            context: encryption context / AAD dictionary

        Returns:

            plaintext

        """
        pass


def get_keys():
    return (
        os.environ['ABNOSQL_KMS_KEYS'].split(',')
        if 'ABNOSQL_KMS_KEYS' in os.environ
        else None
    )


def pack_bytes(list_of_bytes: t.List[bytes], max_len: int) -> bytes:
    # pack list of bytearrays
    packed_bytes = bytearray()
    for b in list_of_bytes:
        length = len(b)
        if length > max_len:
            raise ValueError('Byte array is too long')
        packed_bytes += struct.pack('>H', length) + b
    # add 4 for the total_length field itself
    total_length = len(packed_bytes) + 4
    return struct.pack('>I', total_length) + packed_bytes


def unpack_bytes(packed_bytes: bytes) -> t.List[bytes]:
    # unpack list of bytes
    total_length = struct.unpack('>I', packed_bytes[:4])[0]
    if total_length != len(packed_bytes):
        raise ValueError('Data length does not match total_length field')
    packed_bytes = packed_bytes[4:]
    list_of_bytes = []
    i = 0
    while i < len(packed_bytes):
        length = struct.unpack('>H', packed_bytes[i:i+2])[0]
        i += 2
        list_of_bytes.append(packed_bytes[i:i+length])
        i += length
    return list_of_bytes


def kms(
    config: t.Optional[dict] = None,
    provider: t.Optional[str] = None
) -> KmsBase:
    """Create kms object

    Args:

        config: optional config
        database: optional database

    Returns:
        KmsBase object

    """
    if provider is None:
        provider = os.environ.get('ABNOSQL_KMS')

    if provider is None:
        raise ex.PluginException('kms plugin provider not defined')

    pm = plugin.get_pm('kms')
    module = pm.get_plugin(provider)
    if module is None:
        raise ex.PluginException(f'kms.{provider} plugin not found')
    if hasattr(module, 'MISSING_DEPS'):
        raise ex.PluginException(
            f'kms.{provider} plugin missing dependencies'
        )
    return module.Kms(pm, config)
