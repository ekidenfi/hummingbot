import asyncio
import time
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.derivative.ekiden_perpetual import ekiden_perpetual_constants as CONSTANTS
from hummingbot.connector.derivative.ekiden_perpetual.ekiden_perpetual_constants import OrderSide
from hummingbot.connector.derivative.ekiden_perpetual.ekiden_perpetual_utils import get_scale_factors
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.funding_info import FundingInfo, FundingInfoUpdate
from hummingbot.core.data_type.order_book import OrderBookMessage
from hummingbot.core.data_type.order_book_message import OrderBookMessageType
from hummingbot.core.data_type.perpetual_api_order_book_data_source import PerpetualAPIOrderBookDataSource
from hummingbot.core.utils.tracking_nonce import NonceCreator
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant

if TYPE_CHECKING:
    from hummingbot.connector.derivative.ekiden_perpetual.ekiden_perpetual_derivative import EkidenPerpetualDerivative


class EkidenPerpetualAPIOrderBookDataSource(PerpetualAPIOrderBookDataSource):
    def __init__(
        self,
        trading_pairs: List[str],
        connector: "EkidenPerpetualDerivative",
        api_factory: WebAssistantsFactory,
        domain: str = CONSTANTS.DOMAIN,
    ):
        super().__init__(trading_pairs)
        self._connector = connector
        self._api_factory = api_factory
        self._domain = domain
        self._nonce_provider = NonceCreator.for_microseconds()

    async def get_last_traded_prices(
        self, trading_pairs: List[str], domain: Optional[str] = None
    ) -> Dict[str, float]:
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    async def get_funding_info(self, trading_pair: str) -> FundingInfo:
        symbol = await self._connector.exchange_symbol_associated_to_pair(
            trading_pair=trading_pair
        )
        response: Dict[str, Any] = await self._request_pair_funding_info(symbol)

        timestamp_str = response["next_funding_time"]
        dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
        unix_ts = dt.timestamp()
        unix_ts_ms = int(unix_ts * 1000)
        trading_rule = self._connector.trading_rules[trading_pair]
        price_factor, _ = get_scale_factors(trading_rule, inverse=True)

        scaled_oracle_p = response["oracle_price"] * price_factor
        scaled_mark_p = response["mark_price"] * price_factor

        rate = Decimal(response["funding_rate_percentage"])

        funding_info = FundingInfo(
            trading_pair=trading_pair,
            index_price=scaled_oracle_p,
            mark_price=scaled_mark_p,
            next_funding_utc_timestamp=unix_ts_ms,
            rate=rate,
        )
        return funding_info

    async def _request_pair_funding_info(self, trading_pair: str) -> Dict[str, Any]:
        market_addr = await self._connector.market_address_associated_to_pair(
            trading_pair
        )
        path_url = CONSTANTS.MARKET_FUNDING + f"/{market_addr}"

        data = await self._connector._api_get(
            path_url=path_url, limit_id=CONSTANTS.MARKET_FUNDING
        )
        return data

    async def _subscribe_channels(self, ws: WSAssistant):
        try:
            for trading_pair in self._trading_pairs:
                market_addr = await self._connector.market_address_associated_to_pair(
                    trading_pair=trading_pair
                )
                order_book_payload = {
                    "op": "subscribe",
                    "args": [f"orderbook/{market_addr}"],
                    "req_id": f"{self._nonce_provider.get_tracking_nonce()}",
                }
                trades_payload = {
                    "op": "subscribe",
                    "args": [f"trade/{market_addr}"],
                    "req_id": f"{self._nonce_provider.get_tracking_nonce()}",
                }
                ticker_payload = {
                    "op": "subscribe",
                    "args": [f"ticker/{market_addr}"],
                    "req_id": f"{self._nonce_provider.get_tracking_nonce()}",
                }

                subscribe_orderbook_request: WSJSONRequest = WSJSONRequest(
                    payload=order_book_payload
                )
                subscribe_trade_request: WSJSONRequest = WSJSONRequest(
                    payload=trades_payload
                )
                subscribe_ticker_request: WSJSONRequest = WSJSONRequest(
                    payload=ticker_payload
                )

                await ws.send(subscribe_orderbook_request)
                await ws.send(subscribe_trade_request)
                await ws.send(subscribe_ticker_request)

                self.logger().info("Subscribed to public order book, trade channels...")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error(
                "Unexpected error occurred subscribing to order book data streams."
            )
            raise

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant):
        while True:
            try:
                await super()._process_websocket_messages(
                    websocket_assistant=websocket_assistant
                )
            except asyncio.TimeoutError:
                ping_request = WSJSONRequest(payload={"op": "ping"})
                await websocket_assistant.send(ping_request)

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        channel = ""

        op = event_message["op"]
        match op:
            case "subscribed":
                pass
            case "unsubscribed":
                self.logger().info("Unsubscribed")
            case "pong":
                self.logger().info("Received pong")
            case "error":
                msg = event_message["message"]
                self.logger().error(f"Error from order book ws_stream: , msg={msg}")
            case "event":
                topic: str = event_message["topic"].split("/")[0]
                match topic:
                    case CONSTANTS.WS_TRADES:
                        channel = self._trade_messages_queue_key
                    case CONSTANTS.WS_ORDERBOOK:
                        channel = self._diff_messages_queue_key
                    case CONSTANTS.WS_TICKER:
                        channel = self._funding_info_messages_queue_key
            case "_":
                self.logger().warning(f"Unrecognized order book ws_stream op: {op}")

        return channel

    async def _parse_order_book_diff_message(
        self, raw_message: Dict[str, Any], message_queue: asyncio.Queue
    ):
        timestamp: float = raw_message["data"]["timestamp"] * 1e-3
        market_addr = raw_message["data"]["market_addr"]
        trading_pair = await self._connector.trading_pair_associated_to_market_address(
            market_addr
        )
        trading_rule = self._connector.trading_rules[trading_pair]
        price_factor, amount_factor = get_scale_factors(trading_rule, inverse=True)
        data = raw_message["data"]
        bids = [
            (row[0] * float(price_factor), row[1] * float(amount_factor))
            for row in data["bids"]
        ]
        asks = [
            (row[0] * float(price_factor), row[1] * float(amount_factor))
            for row in data["asks"]
        ]
        order_book_message = OrderBookMessage(
            OrderBookMessageType.DIFF,
            {
                "trading_pair": trading_pair,
                "update_id": data["seq"],
                "bids": bids,
                "asks": asks,
            },
            timestamp=timestamp,
        )
        message_queue.put_nowait(order_book_message)

    async def _parse_trade_message(
        self, raw_message: Dict[str, Any], message_queue: asyncio.Queue
    ):
        data = raw_message["data"]
        trades: List[Dict[str, Any]] = data["trades"]
        trading_pair = await self._connector.trading_pair_associated_to_market_address(
            raw_message["data"]["market_addr"]
        )
        trading_rule = self._connector.trading_rules[trading_pair]
        price_factor, amount_factor = get_scale_factors(trading_rule, inverse=True)

        for trade in trades:
            scaled_price = trade["price"] * price_factor
            scaled_size = trade["size"] * amount_factor
            trade_message: OrderBookMessage = OrderBookMessage(
                OrderBookMessageType.TRADE,
                {
                    "trading_pair": trading_pair,
                    "trade_type": (
                        float(TradeType.SELL.value)
                        if trade["side"] == OrderSide.SELL.value
                        else float(TradeType.BUY.value)
                    ),
                    "trade_id": trade["id"],
                    "price": float(scaled_price),
                    "amount": float(scaled_size),
                },
                timestamp=trade["timestamp"] * 1e-3,
            )

            message_queue.put_nowait(trade_message)

    async def _parse_funding_info_message(
        self, raw_message: Dict[str, Any], message_queue: asyncio.Queue
    ):
        data = raw_message["data"]
        trading_pair = await self._connector.trading_pair_associated_to_market_address(
            data["market_addr"]
        )
        trading_rule = self._connector.trading_rules[trading_pair]
        price_factor, _ = get_scale_factors(trading_rule, inverse=True)

        scaled_index_p = data["index_price"] * price_factor
        scaled_mark_p = data["mark_price"] * price_factor

        funding_info_update = FundingInfoUpdate(
            trading_pair=trading_pair,
            index_price=scaled_index_p,
            mark_price=scaled_mark_p,
            next_funding_utc_timestamp=data["next_funding_time"],
            rate=Decimal(data["funding_rate"]),
        )
        message_queue.put_nowait(funding_info_update)

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        snapshot_msg: OrderBookMessage = OrderBookMessage(
            OrderBookMessageType.SNAPSHOT,
            {
                "trading_pair": trading_pair,
                "update_id": 0,
                "bids": [],
                "asks": [],
            },
            timestamp=time.time(),
        )
        return snapshot_msg

    async def _request_order_book_snapshot(self, trading_pair: str):
        pass  # unused

    async def _connected_websocket_assistant(self) -> WSAssistant:
        ws = await self._api_factory.get_ws_assistant()
        url = f"{CONSTANTS.PERPETUAL_WS_URL}{CONSTANTS.WS_PUBLIC}"
        await ws.connect(ws_url=url)
        return ws
