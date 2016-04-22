#!/usr/bin/env python3

import argparse
import asyncio
import json as js
import logging

from common.api import CommonApi, AppOk
from worker.config import WorkerConfig
from worker.executor import Executors
from worker.resource import Resources


class WorkerApp:
    def __init__(self, config: WorkerConfig):
        self.config = config
        self.executors = Executors(config.plugins.executors_dir)
        self.resources = Resources(config.plugins.resources_dir)

    def hello(self, args):
        return AppOk('Hello')


class WorkerApi(CommonApi):
    def __init__(self, loop, config: WorkerConfig):
        super().__init__(loop=loop, api_config=config.api, app_config=config)

    @staticmethod
    def _create_app(loop, config):
        return WorkerApp(config)

    @property
    def routes(self):
        return [
            ('GET', '/hello', 'hello')
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
