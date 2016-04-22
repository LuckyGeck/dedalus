from util.symver import SymVer
from worker.resource import Resource


class SystemPackageResource(Resource):
    name = 'system_package'
    version = SymVer(0, 0, 1)

    @property
    def is_installed(self) -> bool:
        return False
