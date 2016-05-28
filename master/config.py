from common.api_config import CommonApiConfig
from util.config import Config, ConfigField


class PluginsConfig(Config):
    backends_dir = ConfigField(type=str, required=True, default='plugins/backends')


class MasterConfig(Config):
    api = CommonApiConfig(common_logger='dedalus.master.api.common', access_logger='dedalus.master.api.access')
    backend = ConfigField(type=str, required=True, default='leveldb')
    backend_config = ConfigField(type=dict, required=True, default=dict())
    plugins = PluginsConfig()
