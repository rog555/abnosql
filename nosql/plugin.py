from importlib import import_module
import os
import pkgutil
import typing as ty


class PluginException(Exception):
    pass


class Plugin:

    def __init__(
        self, prefix: str,
        _cls: ty.Protocol,
        path: str = None
    ) -> None:
        self._path = path or ''
        self._prefix = prefix
        self._cls = _cls
        # how to get actual class name of Protocol class without hack?
        self._cls_name = str(_cls).split('.')[-1][0:-2]
        self.load()

    def _cls_funcs(self, _cls: ty.TypeVar('T')):
        allowed = ['__init__']
        return set([
            _ for _ in dir(_cls)
            if (_ in allowed or not _.startswith('_'))
            and callable(getattr(_cls, _))
        ])

    def _load_plugin(self, info) -> None:

        name = info.name.split('.')[-1]
        try:
            print('importing %s' % info.name)
            module = import_module(info.name)
            print('imported')
            if not hasattr(module, self._cls_name):
                return
            _cls = getattr(module, self._cls_name)

            # faster than typing.runtime_checkable and signature
            # not checked anyway, too much faff to add ABCs as well
            actual = self._cls_funcs(_cls)
            expected = self._cls_funcs(self._cls)
            if len(expected.difference(actual)):
                return

            self._plugins[name] = {}
            if (
                hasattr(module, 'MISSING_DEPS')
                and getattr(module, 'MISSING_DEPS') is True
            ):
                raise PluginException('missing dependencies')
            self._plugins[name] = {'cls': _cls}
        except Exception as e:
            self._plugins[name] = {'ex': str(e)}

    def load(self) -> None:
        self._plugins = {}
        if os.path.isdir(self._path):
            for info in pkgutil.iter_modules([self._path]):
                self._load_plugin(info)
        for info in pkgutil.iter_modules():
            if info.name.startswith(self._prefix):
                self._load_plugin(info)

    def get(self, name: ty.Optional[str]) -> ty.TypeVar('T'):
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
