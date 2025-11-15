import time
from typing import Callable, List, Optional

from hummingbot.connector.derivative.ekiden_perpetual import ekiden_perpetual_constants as CONSTANTS
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTRequest
from hummingbot.core.web_assistant.rest_pre_processors import RESTPreProcessorBase
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


class HeadersContentRESTPreProcessor(RESTPreProcessorBase):
    async def pre_process(self, request: RESTRequest) -> RESTRequest:
        request.headers = request.headers or {}
        request.headers["Content-Type"] = "application/json"
        return request


def build_api_factory(
    throttler: Optional[AsyncThrottler] = None,
    time_synchronizer: Optional[TimeSynchronizer] = None,
    time_provider: Optional[Callable] = None,
    auth: Optional[AuthBase] = None,
) -> WebAssistantsFactory:
    throttler = throttler or create_throttler()
    time_synchronizer = time_synchronizer or TimeSynchronizer()
    api_factory = WebAssistantsFactory(
        throttler=throttler,
        auth=auth,
        rest_pre_processors=[
            HeadersContentRESTPreProcessor(),
        ],
    )
    return api_factory


def create_throttler(trading_pairs: List[str] = None) -> AsyncThrottler:
    throttler = AsyncThrottler(CONSTANTS.RATE_LIMITS)
    return throttler


def public_rest_url(path_url: str, domain: str = CONSTANTS.DOMAIN) -> str:
    return CONSTANTS.PERPETUAL_BASE_URL + CONSTANTS.API_VERSION + path_url


def private_rest_url(path_url: str, domain: str = CONSTANTS.DOMAIN) -> str:
    return public_rest_url(path_url, domain)


async def get_current_server_time(
    throttler: Optional[AsyncThrottler] = None,
    domain: str = CONSTANTS.DOMAIN,
) -> float:
    return time.time()
