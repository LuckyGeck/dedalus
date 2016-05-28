#!/usr/bin/env python3

import argparse
import asyncio
import json as js
import logging
from itertools import islice
from uuid import uuid4

from aiohttp.web_reqrep import Request
from common.models.graph import GraphInstanceInfo
from common.api import CommonApi, ResultOk, ResultError
from master.backend import MasterBackends
from master.config import MasterConfig
from master.engine import Engine
from master.scheduler import Scheduler


class MasterApp:
    def __init__(self, config: MasterConfig) -> None:
        self.config = config
        self.backend = MasterBackends(config.plugins.backends_dir).construct_backend(config.backend,
                                                                                     config.backend_config)
        self.engine = Engine(self.backend)
        self.scheduler = Scheduler(self.backend, self.engine)

    def shutdown(self):
        self.scheduler.shutdown()

    @staticmethod
    def ping(*args):
        return ResultOk('pong')

    def list_graphs(self, args: dict, request: Request):
        graph_name = args.get('graph_name', None)
        with_info = (args.get('with_info', '1') == '1')
        limit = int(args.get('limit', '-1'))
        offset = int(args.get('offset', '0'))
        it = islice(self.backend.list_graph_struct(graph_name=graph_name, with_info=with_info),
                    offset, offset + limit if limit >= 0 else None)
        return ResultOk([
            graph_struct.to_json() if graph_struct else {'graph_name': graph_name, 'revision': revision}
            for graph_name, revision, graph_struct in it
        ])

    def create_graph(self, args: dict, request: Request):
        graph_name = request.match_info.get('graph_name', None)
        if not graph_name:
            return ResultError(error='Graph name should be non-empty string')
        rev = self.engine.add_graph_struct(graph_name=graph_name, graph_struct=args)
        return ResultOk({'graph_name': graph_name, 'revision': rev})

    def launch_graph(self, args: dict, request: Request):
        graph_name = request.match_info.get('graph_name', None)
        revision = int(request.match_info.get('revision', -1))
        if not graph_name:
            return ResultError(error='graph_name should be set')
        graph_struct = self.backend.read_graph_struct(graph_name, revision)
        return ResultOk(self.engine.add_graph_instance(uuid4().hex, graph_struct).to_json())

    def list_instances(self, args: dict, request: Request):
        with_info = (args.get('with_info', '1') == '1')
        limit = int(args.get('limit', '-1'))
        offset = int(args.get('offset', '0'))
        it = islice(self.backend.list_graph_instance_info(with_info=with_info),
                    offset, offset + limit if limit >= 0 else None)
        return ResultOk([
            instance_info.to_json() if instance_info else {'instance_id': instance_id}
            for instance_id, instance_info in it
        ])


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
            ('GET', '/v1.0/graphs', 'list_graphs'),
            ('POST', '/v1.0/graph', 'create_graph'),
            ('POST', '/v1.0/graph/{graph_name}/{revision}/launch', 'launch_graph'),
            ('POST', '/v1.0/graph/{graph_name}/launch', 'launch_graph'),
            ('GET', '/v1.0/instances', 'list_instances'),
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
        master.app.shutdown()
        loop.run_until_complete(master.server.wait_closed())
        loop.run_until_complete(master.web_app.finish())

    # TODO(luckygeck): gracefully stop all workers
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
