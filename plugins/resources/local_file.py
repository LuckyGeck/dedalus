import os

from util.config import Config, ConfigField
from util.filehash import get_file_hash
from util.symver import SymVer
from worker.resource import Resource


class LocalFileResourceConfig(Config):
    local_path = ConfigField(type=str, required=True, default=None)


class LocalFileResource(Resource):
    name = 'local_file'
    version = SymVer(0, 0, 1)
    config_class = LocalFileResourceConfig

    @property
    def get_local_version(self) -> str:
        path = self.config.local_path
        return get_file_hash(path) if os.path.exists(path) else None


if __name__ == '__main__':
    res = LocalFileResource(
        {
            'local_path': '/tmp/1.txt'
        }
    )
    print('IsInstalled: ', res.is_installed)
    print('LocalVersion: ', res.get_local_version)
    print('Installing... ')
    res.ensure()
    print('IsInstalled: ', res.is_installed)
    print('LocalVersion: ', res.get_local_version)
