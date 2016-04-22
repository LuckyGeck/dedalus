from common.api_config import CommonApiConfig
from util.config import Config, ConfigField


class PluginsConfig(Config):
    executors_dir = ConfigField(type=str, required=True, default='plugins/executors')
    resources_dir = ConfigField(type=str, required=True, default='plugins/resources')


class WorkerConfig(Config):
    api = CommonApiConfig(common_logger='dedalus.api.common', access_logger='dedalus.api.access')
    backend = ConfigField(type=str, required=True, default='InMemoryBackend')
    plugins = PluginsConfig()