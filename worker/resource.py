import abc

from util.plugins import PluginBase, PluginsMaster
from util.config import Config


class ResourceNonInstallableError(Exception):
    def __init__(self, resource_type: str, resource_config):
        self.resource_type = resource_type
        self.resource_config = resource_config

    def __str__(self):
        return 'Resource {} is not installable. Config: {}'.format(self.resource_type, self.resource_config.to_json())


class Resource(PluginBase, metaclass=abc.ABCMeta):
    def __init__(self, config: dict = None, **kwargs):
        assert config is None or not kwargs, 'Only one of config and kwargs should be set'
        self.config.from_json(kwargs if config is None else config)
        self.config.verify()

    @property
    def is_installed(self) -> bool:
        return self.get_local_version is not None

    def ensure(self):
        if not self.is_installed:
            self.force_install()

    @property
    @abc.abstractmethod
    def config(self) -> Config:
        pass

    @property
    @abc.abstractmethod
    def get_local_version(self) -> str:
        """Returns package version that is currently installed. None if package is not installed."""
        pass

    def force_install(self):
        """Override this method, if resource type supports installation"""
        raise ResourceNonInstallableError(resource_type=self.__class__.__name__, resource_config=self.config)


class Resources(PluginsMaster):
    plugin_base_class = Resource
