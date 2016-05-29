import abc
import functools
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Iterable, Tuple
import traceback

from aiohttp import web
from common.api_config import CommonApiConfig
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

    @property
    def code(self):
        return 200

    def to_dict(self):
        return {'status': self.status, 'payload': self.payload}


class ResultOk(AppResponse):
    status = 'ok'


class ResultError(AppResponse):
    status = 'error'
    code = 500


def json_response(*args, **kwargs):
    kwargs['dumps'] = functools.partial(json.dumps, ensure_ascii=False)
    return web.json_response(*args, **kwargs)


class CommonApi(metaclass=abc.ABCMeta):
    def __init__(self, loop, api_config: CommonApiConfig, app_config):
        self.loop = loop
        self.host = api_config.host
        self.port = api_config.port

        self.logger = logging.getLogger(api_config.common_logger)
        self.access_log = logging.getLogger(api_config.access_logger)
        self.web_app = None
        self.handler = None
        self.server = None

        self.executor = ThreadPoolExecutor()
        self.app = self._create_app(loop, app_config)

    @staticmethod
    @abc.abstractmethod
    def _create_app(loop, app_config):
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
            else:
                args = await request.json() if request.has_body else {}
            result = await self.loop.run_in_executor(self.executor, func, args, request)

        except BackendNetworkError as e:
            self.logger.error('Backend network error: %s', e)
            result = ResultError(code=ErrorCode.backend_network_error, reason=str(e))
        except BackendError as e:
            self.logger.error('Backend error: %s', e)
            result = ResultError(code=ErrorCode.backend_error, reason=str(e))
        except AppError as e:
            self.logger.error('App error: %s', e)
            result = ResultError(code=ErrorCode.app_error, reason=str(e))
        except Exception as e:
            self.logger.error('Exception [%s]: %s; trace: %s', e.__class__.__name__, e, traceback.format_exc())
            result = ResultError(code=ErrorCode.app_error, exception=e.__class__.__name__, reason=str(e))
        return json_response(result.to_dict(), status=result.code)

    def to(self, action):
        # converted to coroutine automatically
        return functools.partial(self._wrap, getattr(self.app, action))

    async def create_server(self):
        self.web_app = web.Application(loop=self.loop)

        self._fill_router()

        self.handler = self.web_app.make_handler(access_log=self.access_log)
        self.server = await self.loop.create_server(self.handler, self.host, self.port)