from common.api_config import CommonApiConfig
from util.config import Config, ConfigField


class PluginsConfig(Config):
    execution_data_root = ConfigField(type=str, required=True, default='/tmp/dedalus-worker-root')
    executors_dir = ConfigField(type=str, required=True, default='plugins/executors')
    resources_dir = ConfigField(type=str, required=True, default='plugins/resources')
    backends_dir = ConfigField(type=str, required=True, default='plugins/backends')


class WorkerConfig(Config):
    api = CommonApiConfig(common_logger='dedalus.worker.api.common',
                          access_logger='dedalus.worker.api.access',
                          port=8081)
    backend = ConfigField(type=str, required=True, default='leveldb')
    backend_config = ConfigField(type=dict, required=True, default=dict())
    plugins = PluginsConfig()
