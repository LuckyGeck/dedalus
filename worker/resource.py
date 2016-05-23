import abc

from common.models.resource import ResourceInfo
from util.config import Config
from util.plugins import PluginBase, PluginsMaster


class ResourceNonInstallableError(Exception):
    def __init__(self, resource_type: str, resource_config) -> None:
        self.resource_type = resource_type
        self.resource_config = resource_config

    def __str__(self):
        return 'Resource {} is not installable. Config: {}'.format(self.resource_type, self.resource_config.to_json())


class Resource(PluginBase, metaclass=abc.ABCMeta):
    def __init__(self, config: dict = None, **kwargs) -> None:
        assert config is None or not kwargs, 'Only one of config and kwargs should be set'
        self.config = self.config_class()
        self.config.from_json(kwargs if config is None else config)
        self.config.verify()

    @property
    def is_installed(self) -> bool:
        return self.get_local_version is not None

    def ensure(self):
        if not self.is_installed:
            self.force_install()

    @classmethod
    @abc.abstractmethod
    def config_class(cls) -> Config:
        return Config()

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

    def construct_resource(self, resource_info: ResourceInfo) -> Resource:
        return self.find_plugin(resource_info.name, resource_info.min_version)(resource_info.config)