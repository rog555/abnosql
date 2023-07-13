from abc import ABCMeta  # type: ignore
from abc import abstractmethod
import os
import typing as t

import pluggy  # type: ignore

import abnosql.exceptions as ex
from abnosql import plugin

hookspec = pluggy.HookspecMarker('abnosql.crypto')


class CryptoBase(metaclass=ABCMeta):
    @abstractmethod
    def __init__(
        self, pm: plugin.PM, config: t.Optional[dict] = None
    ) -> None:
        """Instantiate crypto object

        Args:

            pm: pluggy plugin manager
            config: optional config dict dict
        """
        pass

    @abstractmethod
    def encrypt(self, plaintext: str, context: t.Dict) -> str:
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


def get_key_ids():
    return (
        os.environ['ABNOSQL_KEY_IDS'].split(',')
        if 'ABNOSQL_KEY_IDS' in os.environ
        else None
    )


def crypto(
    config: t.Optional[dict] = None,
    provider: t.Optional[str] = None
) -> CryptoBase:
    if provider is None:
        provider = os.environ.get('ABNOSQL_CRYPTO')
    pm = plugin.get_pm('crypto')
    module = pm.get_plugin(provider)
    if module is None:
        raise ex.PluginException(f'crypto.{provider} plugin not found')
    return module.Crypto(pm, config)
