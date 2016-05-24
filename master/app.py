#!/usr/bin/env python3

import argparse
import asyncio
import json as js
import logging
from itertools import islice
from uuid import uuid4

from aiohttp.web_reqrep import Request
from common.api import CommonApi, ResultOk, ResultError
from master.backend import MasterBackends
from master.config import MasterConfig


class MasterApp:
    def __init__(self, config: MasterConfig) -> None:
        self.config = config
        self.backend = MasterBackends(config.plugins.backends_dir).construct_backend(config.backend,
                                                                                     config.backend_config)

    @staticmethod
    def ping(*args):
        return ResultOk('pong')


class MasterApi(CommonApi):
    def __init__(self, loop, cfg: MasterConfig) -> None:
        super().__init__(loop=loop, api_config=cfg.api, app_config=cfg)

    @staticmethod
    def _create_app(loop, cfg: MasterConfig):
        return MasterApp(cfg)

    @property
    def routes(self):
        return [
            ('GET', '/ping', 'ping'),
        ]


def main(config: MasterConfig, args):
    loop = asyncio.get_event_loop()
    master = MasterApi(loop, config)
    loop.run_until_complete(master.create_server())

    for sock in master.server.sockets:
        master.logger.info('Server started on http://%s:%d', *sock.getsockname()[:2])

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        master.logger.info('Got SIGINT, shutting down...')
    finally:
        loop.run_until_complete(master.handler.finish_connections(1))
        master.server.close()
        loop.run_until_complete(master.server.wait_closed())
        loop.run_until_complete(master.web_app.finish())

    loop.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', help='Path to config', type=argparse.FileType('r'), required=False)
    parser.add_argument('--verbose', action='store_true', default=False)
    args = parser.parse_args()

    level = logging.DEBUG if args.verbose else logging.WARNING
    logging.basicConfig(format='%(asctime)s\t%(levelname)s:\t%(message)s', level=level)

    config = MasterConfig()
    if args.config:
        config.from_json(js.load(args.config))
    print('Config used:', js.dumps(config.to_json(), indent=2))
    main(config, args)
