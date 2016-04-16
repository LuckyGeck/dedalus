from util.symver import SymVer
from worker.resource import Resource


class RemoteFileResource(Resource):
    name = 'file'
    version = SymVer(0, 0, 1)

    def is_installed(self) -> bool:
        return self.config is None
