#!/usr/bin/env python3

import argparse
import asyncio
import json as js
import logging
from itertools import islice
from uuid import uuid4

from aiohttp.web_reqrep import Request
from common.api import CommonApi, ResultOk, ResultError
from common.models.state import TaskState
from worker.backend import WorkerBackends
from worker.config import WorkerConfig
from worker.engine import Engine
from worker.executor import Executors
from worker.resource import Resources


class WorkerApp:
    def __init__(self, config: WorkerConfig) -> None:
        self.config = config
        self.executors = Executors(config.plugins.execution_data_root, config.plugins.executors_dir)
        self.resources = Resources(config.plugins.resources_dir)
        self.backend = WorkerBackends(config.plugins.backends_dir).construct_backend(config.backend,
                                                                                     config.backend_config)
        self.engine = Engine(self.backend, self.resources, self.executors)

    @staticmethod
    def ping(*args):
        return ResultOk('pong')

    def create_task(self, args: dict, request: Request):
        task_id = uuid4().hex
        self.engine.create_idle_task(task_id, task_struct=args)
        return ResultOk({'task_id': task_id})

    def get_task_info(self, args: dict, request: Request):
        task_id = request.match_info.get('task_id', None)
        if not task_id:
            return ResultError(error='task_id field should be set')
        return ResultOk(self.backend.read_task_info(task_id=task_id).to_json())

    def get_task_state(self, args: dict, request: Request):
        task_id = request.match_info.get('task_id', None)
        if not task_id:
            return ResultError(error='task_id field should be set')
        task_info = self.backend.read_task_info(task_id=task_id)
        return ResultOk({'state': task_info.exec_stats.state.name})

    def start_task(self, args: dict, request: Request):
        task_id = request.match_info.get('task_id', None)
        if not task_id:
            return ResultError(error='task_id field should be set')
        return self._set_task_state(task_id, TaskState.preparing)

    def stop_task(self, args: dict, request: Request):
        task_id = request.match_info.get('task_id', None)
        if not task_id:
            return ResultError(error='task_id field should be set')
        return self._set_task_state(task_id, TaskState.stopped)

    def _set_task_state(self, task_id: str, task_state_name: str):
        prev_state = self.engine.set_task_state(task_id, task_state_name)
        return ResultOk(prev_state=prev_state, new_state=task_state_name)

    def list_tasks(self, args: dict, request: Request):
        with_info = (args.get('with_info', '1') == '1')
        limit = int(args.get('limit', '-1'))
        offset = int(args.get('offset', '0'))
        it = islice(self.backend.list_tasks(with_info=with_info), offset, offset + limit if limit >= 0 else None)
        return ResultOk(
            [
                task_info.to_json() if task_info else {'task_id': task_id}
                for task_id, task_info in it
            ]
        )


class WorkerApi(CommonApi):
    def __init__(self, loop, cfg: WorkerConfig) -> None:
        super().__init__(loop=loop, api_config=cfg.api, app_config=cfg)

    @staticmethod
    def _create_app(loop, cfg: WorkerConfig):
        return WorkerApp(cfg)

    @property
    def routes(self):
        return [
            ('GET', '/ping', 'ping'),
            ('GET', '/v1.0/tasks', 'list_tasks'),
            ('POST', '/v1.0/task/', 'create_task'),
            ('GET',  '/v1.0/task/{task_id}', 'get_task_info'),
            ('GET',  '/v1.0/task/{task_id}/state', 'get_task_state'),
            ('POST', '/v1.0/task/{task_id}/start', 'start_task'),
            ('POST', '/v1.0/task/{task_id}/stop', 'stop_task'),
        ]


def main(config: WorkerConfig, args):
    loop = asyncio.get_event_loop()
    worker = WorkerApi(loop, config)
    loop.run_until_complete(worker.create_server())

    for sock in worker.server.sockets:
        worker.logger.info('Server started on http://%s:%d', *sock.getsockname()[:2])

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        worker.logger.info('Got SIGINT, shutting down...')
    finally:
        loop.run_until_complete(worker.handler.finish_connections(1))
        worker.server.close()
        loop.run_until_complete(worker.server.wait_closed())
        loop.run_until_complete(worker.web_app.finish())
    # TODO(luckygeck): gracefully stop all workers
    loop.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', help='Path to config', type=argparse.FileType('r'), required=False)
    parser.add_argument('--verbose', action='store_true', default=False)
    args = parser.parse_args()

    level = logging.DEBUG if args.verbose else logging.WARNING
    logging.basicConfig(format='%(asctime)s\t%(levelname)s:\t%(message)s', level=level)

    config = WorkerConfig()
    if args.config:
        config.from_json(js.load(args.config))
    print('Config used:', js.dumps(config.to_json(), indent=2))
    main(config, args)
