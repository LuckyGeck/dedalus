import abc
from util.plugins import PluginBase, PluginsMaster


class Resource(PluginBase, metaclass=abc.ABCMeta):
    def __init__(self, config: dict = None, **kwargs):
        assert config is None or not kwargs, 'Only one of config and kwargs should be set'
        self.config = kwargs if config is None else config

    @abc.abstractmethod
    def is_installed(self) -> bool:
        pass


class Resources(PluginsMaster):
    plugin_base_class = Resource
