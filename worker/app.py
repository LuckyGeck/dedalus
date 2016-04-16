#!/usr/bin/env python3

import argparse
import asyncio
import logging
import json as js
from worker.config import WorkerConfig
from worker.executor import Executors
from worker.resource import Resources
from common.api import CommonApi, AppOk, AppError


class WorkerApp:
    def __init__(self, config: WorkerConfig):
        self.config = config
        self.executors = Executors(config['PLUGINS']['EXECUTORS_DIR'])
        self.resources = Resources(config['PLUGINS']['RESOURCES_DIR'])

    def hello(self, args):
        return AppOk('Hello')


class WorkerApi(CommonApi):
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
        config.load_from_json(js.load(args.config))
    main(config, args)
