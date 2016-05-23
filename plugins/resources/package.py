import platform

from plumbum import local
from util.config import Config, ConfigField
from util.symver import SymVer
from worker.resource import Resource

cat = local['cat']
cat.cwd = 'worker/tests'

os_family = platform.system()
if os_family == 'Linux':
    apt_cache_policy = local['apt-cache']['policy']
elif os_family == 'Darwin':
    apt_cache_policy = cat
else:
    raise NotImplementedError('Not supported platform: {}'.format(os_family))


def parse_package_policy(out: str):
    lines = out.splitlines()
    if len(lines) < 2:
        raise AptitudeParsingError()
    key, value = lines[1].split(':', maxsplit=1)
    if key.strip().lower() != 'installed':
        raise AptitudeParsingError()
    version = value.strip()
    return version if version != '(none)' else None


class AptitudeParsingError(Exception):
    pass


class SystemPackageResourceConfig(Config):
    package_name = ConfigField(type=str, required=True, default=None)


class SystemPackageResource(Resource):
    name = 'system_package'
    version = SymVer(0, 0, 1)
    config_class = SystemPackageResourceConfig

    @property
    def get_local_version(self) -> str:
        out = apt_cache_policy[self.config['package_name']]()
        return parse_package_policy(out)


if __name__ == '__main__':
    print(SystemPackageResource(package_name='bash').is_installed)
    print(SystemPackageResource(package_name='zsh').is_installed)
