class WorkerConfig(dict):
    def __init__(self):
        self._load_defaults()

    def load_from_json(self, config):
        for k, v in config.items():
            self[k] = v

    def _load_defaults(self):
        self.load_from_json(_WORKER_DEFAULT_CONFIG)


_WORKER_DEFAULT_CONFIG = {
    'BIND_HOST': 'localhost',
    'BIND_PORT': 8080,

    'BACKEND': 'InMemoryBackend',
    'HEARTBEAT_TIMEOUT': 300,

    'PLUGINS': {
        'EXECUTORS_DIR': 'plugins/executors',
        'RESOURCES_DIR': 'plugins/resources',
    }
}
