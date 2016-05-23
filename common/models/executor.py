from util.config import Config, ConfigField
from util.symver import SymVer


class ExecutorInfo(Config):
    name = ConfigField(type=str, required=True, default='shell')
    min_version = SymVer()
    config = ConfigField(type=dict, required=True, default={})
