import os
from urllib.request import urlretrieve

from util.config import Config, ConfigField
from util.filehash import get_file_hash
from util.symver import SymVer
from worker.resource import Resource


class RemoteFileResourceConfig(Config):
    local_path = ConfigField(type=str, required=True, default=None)
    remote_url = ConfigField(type=str, required=True, default=None)
    extract_after_download = ConfigField(type=bool, required=False, default=False)


class RemoteFileResource(Resource):
    name = 'remote_file'
    version = SymVer(0, 0, 1)
    config_class = RemoteFileResourceConfig

    @property
    def get_local_version(self) -> str:
        path = self.config.local_path
        return get_file_hash(path) if os.path.exists(path) else None

    def force_install(self):
        urlretrieve(self.config.remote_url, self.config.local_path)


if __name__ == '__main__':
    res = RemoteFileResource(
        {
            'local_path': '/tmp/1.txt',
            'remote_url': 'http://google.com/robots.txt'
        }
    )
    print('IsInstalled: ', res.is_installed)
    print('LocalVersion: ', res.get_local_version)
    print('Installing... ')
    res.ensure()
    print('IsInstalled: ', res.is_installed)
    print('LocalVersion: ', res.get_local_version)
