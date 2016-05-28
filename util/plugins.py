import abc
import inspect
import logging
import pkgutil
from os.path import join

from util.symver import SymVer


class PluginWithNameNotFound(Exception):
    def __init__(self, name: str):
        self.plugin_name = name

    def __str__(self):
        return 'Plugin with name \'{}\' not found!'.format(self.plugin_name)


class PluginWithVersionNotFound(Exception):
    def __init__(self, name: str, version: SymVer):
        self.plugin_name = name
        self.plugin_version = version

    def __str__(self):
        return 'Plugin \'{}\' for version {} not found!'.format(self.plugin_name, self.plugin_version)


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
    def __init__(self, plugins_folder: str = None) -> None:
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

    def find_plugin(self, name: str, needed_version: SymVer) -> plugin_base_class:
        if name not in self.plugins:
            raise PluginWithNameNotFound(name)
        # TODO(luckygeck) implement more version restrictions
        version, plugin = max(self.plugins[name].items(), key=lambda _: _[0])
        if version >= needed_version:
            return plugin
        else:
            raise PluginWithVersionNotFound(name, needed_version)

    @classmethod
    def _get_module_plugins(cls, module):
        def is_plugin(c):
            return inspect.isclass(c) \
                   and issubclass(c, cls.plugin_base_class) \
                   and not issubclass(cls.plugin_base_class, c)

        return inspect.getmembers(module, is_plugin)
