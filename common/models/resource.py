from util.config import Config, ConfigField
from util.symver import SymVer


class ResourceInfo(Config):
    name = ConfigField(type=str, required=True, default=None)
    min_version = SymVer()
    config = ConfigField(type=dict, required=True, default={})
