import abc

from util.plugins import PluginBase, PluginsMaster
from util.config import Config


class Executor(PluginBase, metaclass=abc.ABCMeta):
    def __init__(self, config: dict = None, **kwargs):
        assert config is None or not kwargs, 'Only one of config and kwargs should be set'
        self.config.from_json(kwargs if config is None else config)
        self.config.verify()

    @property
    @abc.abstractmethod
    def config(self) -> Config:
        pass

    @abc.abstractmethod
    def start(self):
        pass

    @abc.abstractmethod
    def ping(self):
        pass


class Executors(PluginsMaster):
    plugin_base_class = Executor
