import logging
import os
import typing as ty

from nosql.plugin import Plugin
from nosql import plugins

__version__ = '0.0.1'


logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class Table(ty.Protocol):

    def __init__(self, name: str, config: ty.Optional[dict] = None) -> None:
        ...

    def get_item(self, key: str, partition_key: ty.Optional[str]) -> ty.Dict:
        ...

    def query(self, query: str) -> ty.Dict:
        ...

    def put_item(self, item: ty.Dict) -> bool:
        ...

    def put_items(self, items: ty.Iterable[ty.Dict]) -> bool:
        ...

    def delete_item(self, key: str, partition_key: ty.Optional[str]) -> bool:
        ...


def table(
    name: str, config:
    ty.Optional[dict] = None,
    database: str = None,
    validate: bool = True
):
    if database is None:
        database = os.environ.get('NOSQL_DB')
    return Plugin(
        'nosql_', Table, ns_pkg=plugins, validate=validate
    ).get(database)(name, config)
