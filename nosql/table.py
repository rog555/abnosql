from abc import ABCMeta  # type: ignore
from abc import abstractmethod
import os
import typing as t

import pluggy  # type: ignore

from nosql import plugin

hookimpl = pluggy.HookimplMarker('nosql.table')
hookspec = pluggy.HookspecMarker('nosql.table')


class TableSpecs(plugin.PluginSpec):

    @hookspec(firstresult=True)
    def config(self) -> t.Dict:  # type: ignore[empty-body] # noqa E501
        pass

    @hookspec(firstresult=True)
    def get_item_post(self, table: str, item: t.Dict) -> t.Dict:  # type: ignore[empty-body] # noqa E501
        pass

    @hookspec
    def put_item_post(self, table: str, item: t.Dict) -> None:  # type: ignore[empty-body] # noqa E501
        pass

    @hookspec
    def put_items_post(self, table: str, items: t.List[t.Dict]) -> None:  # type: ignore[empty-body] # noqa E501
        pass

    @hookspec
    def delete_item_post(self, table: str, key: t.Dict) -> None:  # type: ignore[empty-body] # noqa E501
        pass


class TableBase(metaclass=ABCMeta):
    @abstractmethod
    def __init__(
        self, pm: plugin.PM, name: str, config: t.Optional[dict] = None
    ) -> None:
        pass

    @abstractmethod
    def get_item(self, **kwargs) -> t.Dict:
        pass

    @abstractmethod
    def put_item(self, item: t.Dict) -> bool:
        pass

    @abstractmethod
    def put_items(self, items: t.Iterable[t.Dict]) -> bool:
        pass

    @abstractmethod
    def delete_item(self, **kwargs) -> bool:
        pass

    @abstractmethod
    def query(self, query: str) -> t.Iterable[t.Dict]:
        pass


def table(
    name: str, config:
    t.Optional[dict] = None,
    database: t.Optional[str] = None
) -> TableBase:
    if database is None:
        database = os.environ.get('NOSQL_DB')
    pm = plugin.get_pm('table')
    module = pm.get_plugin(database)
    if module is None:
        raise plugin.PluginException(f'table.{database} plugin not found')
    return module.Table(pm, name, config)
