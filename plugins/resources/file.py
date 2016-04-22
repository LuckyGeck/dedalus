import os

from util.symver import SymVer
from worker.resource import Resource


class LocalFileResource(Resource):
    name = 'local_file'
    version = SymVer(0, 0, 1)

    def is_installed(self) -> bool:
        return os.path.exists(self.config['path'])


class RemoteFileResource(Resource):
    name = 'remote_file'
    version = SymVer(0, 0, 1)

    def is_installed(self) -> bool:
        return self.config['path'] is None
