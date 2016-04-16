import abc
import logging
import functools
import json
from concurrent.futures import ThreadPoolExecutor
from typing import Iterable, Tuple

from aiohttp import web
from common.exceptions import BackendError, BackendNetworkError, AppError
from util.enum import Enum


class ErrorCode(metaclass=Enum):
    values = (
        'app_error',
        'backend_error',
        'backend_network_error',
        'concurrency_error',
    )


class AppResponse(metaclass=abc.ABCMeta):
    def __init__(self, *args, **kwargs):
        if len(args) == 0:
            self.payload = kwargs
        elif len(args) == 1 and not kwargs:
            self.payload = args[0]
        else:
            raise Exception('you should pass either one arg, or several kwargs')

    @abc.abstractproperty
    def status(self):
        pass

    def to_dict(self):
        return {'status': self.status, 'payload': self.payload}


class AppOk(AppResponse):
    status = 'ok'


class AppError(AppResponse):
    status = 'error'


def json_response(*args, **kwargs):
    kwargs['dumps'] = functools.partial(json.dumps, ensure_ascii=False)
    return web.json_response(*args, **kwargs)


class CommonApi(metaclass=abc.ABCMeta):
    def __init__(self, loop, config):
        self.loop = loop
        self.host = config['BIND_HOST']
        self.port = config['BIND_PORT']

        self.logger = logging.getLogger('dedalus.worker.api')
        self.access_log = logging.getLogger('dedalus.worker.access_log')
        self.web_app = None
        self.handler = None
        self.server = None

        self.executor = ThreadPoolExecutor()
        self.app = self._create_app(loop, config)

    @staticmethod
    @abc.abstractmethod
    def _create_app(loop, config):
        """Should create an instance of app"""
        pass

    @property
    @abc.abstractmethod
    def routes(self) -> Iterable[Tuple[str, str, str]]:
        """Returns iterable collection of tuples: [(method, url, handler_name), ...]"""
        pass

    def _fill_router(self):
        router = self.web_app.router
        for method, url, handler in self.routes:
            router.add_route(method, url, self.to(handler))

    async def _wrap(self, func, request):
        try:
            if request.method == 'GET':
                # duplicate items will be lost
                args = dict(request.GET.items())

            if request.method == 'POST':
                args = await request.json() if request.has_body else {}

            result = await self.loop.run_in_executor(self.executor, func, args)

        except BackendNetworkError as e:
            self.logger.error('Backend network error: %s', e)
            result = AppError(code=ErrorCode.backend_network_error, reason=str(e))

        except BackendError as e:
            self.logger.error('Backend error: %s', e)
            result = AppError(code=ErrorCode.backend_error, reason=str(e))

        except AppError as e:
            self.logger.error('App error: %s', e)
            result = AppError(code=ErrorCode.app_error, reason=str(e))

        return json_response(result.to_dict())

    def to(self, action):
        # converted to coroutine automatically
        return functools.partial(self._wrap, getattr(self.app, action))

    async def create_server(self):
        self.web_app = web.Application(loop=self.loop)

        self._fill_router()

        self.handler = self.web_app.make_handler(access_log=self.access_log)
        self.server = await self.loop.create_server(self.handler, self.host, self.port)