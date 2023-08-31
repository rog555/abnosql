# generic pluggy loader
from importlib import import_module
import os
from pkgutil import iter_modules
import typing as t

import pluggy  # type: ignore


PKG_ROOT = os.path.dirname(os.path.abspath(__file__))
PKG_NAME = os.path.basename(PKG_ROOT)
_PMS = {}  # type: ignore


class PluginSpec:
    ...


class PluginImpl:
    ...


# class PluginException(Exception):
#     ...


def hookimpl(entity: str, **kwargs) -> pluggy.HookimplMarker:
    return pluggy.HookimplMarker(f'{PKG_NAME}.{entity}', **kwargs)


class PM(pluggy.PluginManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


def clear_pms():
    global _PMS
    _PMS = {}


def get_pm(
    entity: str,
    prefix: t.Optional[str] = None,
    nocache: t.Optional[bool] = False
) -> pluggy.PluginManager:
    """Generic pluggy loader for loading specs, hooks and plugins

    plugins/hooks etc loaded into {mypkg}.{entity} namespace
    default prefix = entity.title()

    Example structure:

    mypkg
    ├─ foo.py            # contains FooSpecs, FooHooks, FooBase abc
    ├- bar.py            # contains BarSpecs, BarHooks, BarBase abc
    ├─ plugin.py         # this file
    └─ plugins
       └─ foo            # entity 'foo'
          └- plugin1.py  # contains Foo(FooBase)
          └- plugin2.py
       └─ bar            # entity 'bar'
          └- plugin1.py  # contains Bar(BarBase)
          └- plugin2.py

    Example usage:

    from mypkg.plugin import get_pm
    foo_pm = get_pm('foo')         # namespace mypkg.foo
    foo_pm.list_name_plugin()      # .. plugin1, plugin2

    bar_pm = get_pm('bar', 'Bar')  # namespace mypkg.bar

    """
    if prefix is None:
        prefix = entity.title()

    # global... cache... yikes...  yuk!
    if not nocache:
        global _PMS
        if _PMS is None:
            _PMS = {}
        if entity in _PMS:
            return _PMS[entity]

    pm = pluggy.PluginManager(f'{PKG_NAME}.{entity}')
    entity_module = import_module(f'{PKG_NAME}.{entity}')

    spec_module = getattr(entity_module, f'{prefix}Specs', None)
    if spec_module and issubclass(spec_module, PluginSpec):  # type: ignore
        pm.add_hookspecs(spec_module)

    for info in iter_modules([os.path.join(PKG_ROOT, 'plugins', entity)]):
        path = f'{PKG_NAME}.plugins.{entity}.{info.name}'
        module = import_module(path)
        pm.register(plugin=module, name=info.name)

    pm.load_setuptools_entrypoints(f'{PKG_NAME}.{entity}')
    pm.check_pending()

    if not nocache:
        _PMS[entity] = pm  # type: ignore
    return pm
