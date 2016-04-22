import abc
import inspect
import logging
import pkgutil
from os.path import join

from util.symver import SymVer


class PluginWithNameNotFound(Exception):
    pass


class PluginWithVersionNotFound(Exception):
    pass


class PluginBase(metaclass=abc.ABCMeta):
    @property
    @abc.abstractmethod
    def name(self) -> str:
        pass

    @property
    @abc.abstractmethod
    def version(self) -> SymVer:
        pass


class PluginsMaster(metaclass=abc.ABCMeta):
    def __init__(self, plugins_folder: str = None):
        assert issubclass(self.plugin_base_class, PluginBase), \
            '{}.plugin_base_class should be a subclass of PluginBase class'.format(self.__class__.__name__)
        self.plugins = dict()
        if plugins_folder:
            self.add_plugins(plugins_folder)

    @property
    @abc.abstractmethod
    def plugin_base_class(self) -> PluginBase:
        pass

    def add_plugins(self, folder: str):
        for loader, module_name, _ in pkgutil.iter_modules([folder]):
            module = loader.find_module(module_name).load_module(module_name)
            for _, plugin in self._get_module_plugins(module):
                logging.info('Found {} plugin "{}" of {} in {}'.format(self.plugin_base_class.__name__,
                                                                       plugin.name,
                                                                       plugin.version,
                                                                       join(folder, module_name)))
                self.plugins.setdefault(plugin.name, dict())[plugin.version] = plugin

    def find_plugin(self, name: str, needed_version: SymVer):
        if name not in self.plugins:
            raise PluginWithNameNotFound()
        # TODO(luckygeck) implement more version restrictions
        version, plugin = max(self.plugins[name].items(), key=lambda _: _[0])
        if version >= needed_version:
            return plugin

    @classmethod
    def _get_module_plugins(cls, module):
        def is_plugin(c):
            return inspect.isclass(c) \
                   and issubclass(c, cls.plugin_base_class) \
                   and not issubclass(cls.plugin_base_class, c)

        return inspect.getmembers(module, is_plugin)
