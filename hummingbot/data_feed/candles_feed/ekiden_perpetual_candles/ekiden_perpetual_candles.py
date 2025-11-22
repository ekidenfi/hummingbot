import logging
from typing import List, Optional

from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.data_feed.candles_feed.candles_base import CandlesBase
from hummingbot.data_feed.candles_feed.ekiden_perpetual_candles import constants as CONSTANTS
from hummingbot.logger import HummingbotLogger


class EkidenPerpetualCandles(CandlesBase):
    _logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self, trading_pair: str, interval: str = "1m", max_records: int = 150):
        super().__init__(trading_pair, interval, max_records)

    @property
    def name(self):
        return f"ekiden_perpetual_{self._trading_pair}"

    @property
    def rest_url(self):
        return CONSTANTS.REST_URL

    @property
    def wss_url(self):
        return CONSTANTS.WSS_URL

    @property
    def health_check_url(self):
        return self.rest_url + CONSTANTS.HEALTH_CHECK_ENDPOINT

    @property
    def candles_url(self):
        return self.rest_url + CONSTANTS.CANDLES_ENDPOINT

    @property
    def candles_endpoint(self):
        return CONSTANTS.CANDLES_ENDPOINT

    @property
    def candles_max_result_per_rest_request(self):
        return CONSTANTS.MAX_RESULTS_PER_CANDLESTICK_REST_REQUEST

    @property
    def rate_limits(self):
        return CONSTANTS.RATE_LIMITS

    @property
    def intervals(self):
        return CONSTANTS.INTERVALS

    async def check_network(self) -> NetworkStatus:
        rest_assistant = await self._api_factory.get_rest_assistant()
        await rest_assistant.execute_request(
            url=self.health_check_url,
            throttler_limit_id=CONSTANTS.HEALTH_CHECK_ENDPOINT,
        )
        return NetworkStatus.CONNECTED

    def get_exchange_trading_pair(self, trading_pair):
        return trading_pair

    @property
    def _is_first_candle_not_included_in_rest_request(self):
        return False

    @property
    def _is_last_candle_not_included_in_rest_request(self):
        return False

    def _get_rest_candles_params(
        self,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: Optional[int] = CONSTANTS.MAX_RESULTS_PER_CANDLESTICK_REST_REQUEST,
    ) -> dict:
        params = {
            "market_addr": self.market_address,
            "timeframe": CONSTANTS.INTERVALS[self.interval],
            "per_page": limit,
        }
        if start_time:
            params["start_time"] = start_time * 1000
        if end_time:
            params["end_time"] = end_time * 1000
        return params

    def _parse_rest_candles(
        self, data: List[dict], end_time: Optional[int] = None
    ) -> List[List[float]]:
        if not data:
            return []

        parsed = []
        for c in data:
            parsed.append([
                self.ensure_timestamp_in_seconds(c["timestamp"]),
                c["open"],
                c["high"],
                c["low"],
                c["close"],
                c["volume"],
                0.0,
                0.0,
                0.0,
                0.0,
            ])
        return parsed[::-1]

    async def initialize_exchange_data(self):
        url = CONSTANTS.REST_URL + CONSTANTS.MARKET_ENDPOINT
        rest_assistant = await self._api_factory.get_rest_assistant()
        market_info = await rest_assistant.execute_request(
            url=url, throttler_limit_id=CONSTANTS.MARKET_ENDPOINT
        )
        for symbol_data in market_info:
            if self._trading_pair == symbol_data["symbol"]:
                self.market_address = symbol_data["addr"]
                return
        raise RuntimeError

    async def listen_for_subscriptions(self):
        pass  # not supported
