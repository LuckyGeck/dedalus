import abc
import pkgutil
import inspect
import logging
from os.path import join
from util.symver import SymVer


class Executor(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def execute(self):
        pass

    @staticmethod
    @property
    @abc.abstractmethod
    def name() -> str:
        return 'base_executor'

    @staticmethod
    @property
    @abc.abstractmethod
    def version() -> SymVer:
        return SymVer()


class ExecutorNameNotFound(Exception):
    pass


class ExecutorVersionNotFound(Exception):
    pass


class Executors:
    def __init__(self, plugins_folder: str = None):
        self.plugins = dict()
        if plugins_folder:
            self.add_plugins(plugins_folder)

    def add_plugins(self, folder: str):
        for loader, module_name, _ in pkgutil.iter_modules([folder]):
            module = loader.find_module(module_name).load_module(module_name)
            for _, plugin in self._get_plugins(module):
                logging.info('Found executor "{}" in {}'.format(plugin.name, join(folder, module_name)))
                self.plugins.setdefault(plugin.name, dict())[plugin.version] = plugin

    @staticmethod
    def _get_plugins(module):
        return inspect.getmembers(module,
                                  lambda c: inspect.isclass(c) and issubclass(c, Executor) and not issubclass(
                                      Executor, c))

    def find_plugin(self, name: str, needed_version: SymVer):
        if name not in self.plugins:
            raise ExecutorNameNotFound()
        # TODO(luckygeck) implement more version restrictions
        version, plugin = max(self.plugins[name].items(), key=lambda _: _[0])
        if version >= needed_version:
            return plugin
