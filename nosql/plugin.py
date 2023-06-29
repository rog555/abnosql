from importlib import import_module
import logging
import pkgutil
from traceback import print_exc
from types import ModuleType
import typing as ty

T = ty.TypeVar('T')


class PluginException(Exception):
    pass


class Plugin:

    def __init__(
        self, prefix: str,
        _cls: ty.Protocol,
        ns_pkg: ModuleType = None,
        validate: bool = True,
        print_ex: bool = True
    ) -> None:
        self._ns_pkg = ns_pkg
        self._prefix = prefix
        self._cls = _cls
        # how to get actual class name of Protocol class without hack?
        self._cls_name = str(_cls).split('.')[-1][0:-2]
        self.validate = validate
        self.print_ex = print_ex
        self.load()

    def _cls_funcs(self, _cls: T):
        allowed = ['__init__']
        return set([
            _ for _ in dir(_cls)
            if (_ in allowed or not _.startswith('_'))
            and callable(getattr(_cls, _))
        ])

    def _load_plugin(self, name) -> None:
        _name = name.split('.')[-1].replace('nosql_', '')
        try:
            module = import_module(name)
            if not hasattr(module, self._cls_name):
                return
            _cls = getattr(module, self._cls_name)

            # faster than typing.runtime_checkable and signature
            # not checked anyway, too much faff to add ABCs as well
            if self.validate:
                actual = self._cls_funcs(_cls)
                expected = self._cls_funcs(self._cls)
                missing = expected.difference(actual)
                if len(missing):
                    logging.warning(f'plugin {name} missing funcs {missing}')
                    return
            self._plugins[_name] = {}
            if (
                hasattr(module, 'MISSING_DEPS')
                and getattr(module, 'MISSING_DEPS') is True
            ):
                raise PluginException('missing dependencies')
            self._plugins[_name] = {'cls': _cls}
        except Exception as e:
            if self.print_ex:
                print_exc()
            self._plugins[_name] = {'ex': str(e)}

    def load(self) -> None:
        self._plugins = {}
        # load from nosql
        if isinstance(self._ns_pkg, ModuleType):
            for _, name, _ in pkgutil.iter_modules(
                self._ns_pkg.__path__, self._ns_pkg.__name__ + '.'
            ):
                self._load_plugin(name)
        # load 3rd party plugins from PYTHONPATH matching nosql_*
        for _, name, _ in pkgutil.iter_modules():
            if name.startswith(self._prefix):
                self._load_plugin(name)

    def get(self, name: ty.Optional[str]) -> T:
        available = ', '.join(self.loaded())
        pd = self._plugins.get(name)
        if pd is None:
            raise PluginException(
                f'plugin {name} not found, available: {available}'
            )
        ex = pd.get('ex')
        if ex:
            raise PluginException(f'plugin {name} exception: {ex}')
        return pd['cls']

    def loaded(self) -> ty.List[str]:
        return [
            name for name in self._plugins
        ]
