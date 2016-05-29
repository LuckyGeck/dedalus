from util.config import Config, ConfigField


class CommonApiConfig(Config):
    host = ConfigField(type=str, required=True, default='localhost')
    port = ConfigField(type=int, required=True, default=8080)
    common_logger = ConfigField(type=str, required=False, default='dedalus.api.common')
    access_logger = ConfigField(type=str, required=False, default='dedalus.api.access')
