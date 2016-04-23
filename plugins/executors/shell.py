import signal

from util.symver import SymVer
from worker.executor import Executor
from util.config import Config, ConfigField

from plumbum import local


class ShellExecutorConfig(Config):
    work_dir = ConfigField(type=str, required=True, default='/tmp')
    command = ConfigField(type=list, required=True, default=None)


class NoCommandRunningError(Exception):
    pass


class ShellExecutor(Executor):
    name = 'shell'
    version = SymVer(0, 0, 1)
    config = ShellExecutorConfig()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._subproc = None

    def start(self):
        if not self._subproc:
            cmd = local
            for _ in self.config.command:
                cmd = cmd[_]
            self._subproc = cmd.popen(cwd=self.config.work_dir)
        return self._subproc

    def ping(self):
        if self._subproc:
            return self._subproc.poll()
        else:
            raise NoCommandRunningError()

    def send_signal(self, sig: int = signal.SIGTERM):
        self._subproc.send_signal(sig=sig)

    def read_log(self):
        return list(self._subproc.iter_lines())


if __name__ == '__main__':
    executor = ShellExecutor({
        'work_dir': '/tmp',
        'command': ['ls', '-la']
    })
    p = executor.start()
    print(executor.ping())
    for _ in range(10000):
        ping = executor.ping()
        if ping == 0:
            print(_)
            break
    print('\n'.join(map(str, list(p))))
