import asyncio
import time
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.derivative.ekiden_perpetual import ekiden_perpetual_constants as CONSTANTS
from hummingbot.connector.derivative.ekiden_perpetual.ekiden_perpetual_constants import OrderSide
from hummingbot.connector.derivative.ekiden_perpetual.ekiden_perpetual_utils import get_funding_timestamp
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
        ticker_response = await self._connector._api_get(
            path_url=CONSTANTS.MARKET_STATS,
            params={"symbol": symbol},
            limit_id=CONSTANTS.MARKET_STATS,
        )
        ticker_list = ticker_response.get("list", [])
        if len(ticker_list) == 0:
            raise ValueError(f"No ticker found for {trading_pair}")

        data = ticker_list[0]
        index_price = Decimal(data["index_price"])
        mark_price = Decimal(data["mark_price"])
        next_funding_time = data["next_funding_time"]
        rate = Decimal(data["funding_rate"])

        funding_info = FundingInfo(
            trading_pair=trading_pair,
            index_price=index_price,
            mark_price=mark_price,
            next_funding_utc_timestamp=next_funding_time,
            rate=rate,
        )
        return funding_info

    async def _request_pair_funding_info(self, symbol: str) -> Dict[str, Any]:
        start_time = get_funding_timestamp(switch=False)
        data = await self._connector._api_get(
            path_url=CONSTANTS.MARKET_FUNDING,
            params={"symbol": symbol, "start_time": str(start_time)},
            limit_id=CONSTANTS.MARKET_FUNDING,
        )
        return data

    async def _subscribe_channels(self, ws: WSAssistant):
        try:
            for trading_pair in self._trading_pairs:
                exchange_symbol = (
                    await self._connector.exchange_symbol_associated_to_pair(
                        trading_pair=trading_pair
                    )
                )
                order_book_payload = {
                    "op": "subscribe",
                    "args": [f"orderbook.200.{exchange_symbol}"],
                    "req_id": f"{self._nonce_provider.get_tracking_nonce()}",
                }
                trades_payload = {
                    "op": "subscribe",
                    "args": [f"trade.{exchange_symbol}"],
                    "req_id": f"{self._nonce_provider.get_tracking_nonce()}",
                }
                ticker_payload = {
                    "op": "subscribe",
                    "args": [f"ticker.{exchange_symbol}"],
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
                topic: str = event_message["topic"].split(".")[0]
                match topic:
                    case CONSTANTS.WS_TRADES:
                        channel = self._trade_messages_queue_key
                    case CONSTANTS.WS_ORDERBOOK:
                        type = event_message["type"]
                        if type == "delta":
                            channel = self._diff_messages_queue_key
                        elif type == "snapshot":
                            channel = self._snapshot_messages_queue_key
                        else:
                            self.logger().warning(
                                f"Unrecognized order book ws_stream type: {type}"
                            )
                    case CONSTANTS.WS_TICKER:
                        channel = self._funding_info_messages_queue_key
                    case _:
                        self.logger().warning(
                            f"Unrecognized order book ws_stream type: {type}"
                        )
            case "_":
                self.logger().warning(f"Unrecognized order book ws_stream op: {op}")

        return channel

    async def _parse_order_book_diff_message(
        self, raw_message: Dict[str, Any], message_queue: asyncio.Queue
    ):
        data = raw_message["data"]
        timestamp: float = data["ts"] * 1e-3
        trading_pair = data["s"]
        bids = [(float(row[0]), float(row[1])) for row in data["b"]]
        asks = [(float(row[0]), float(row[1])) for row in data["a"]]
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
        trading_pair = data[0]["s"]

        for trade in data:
            trade_message: OrderBookMessage = OrderBookMessage(
                OrderBookMessageType.TRADE,
                {
                    "trading_pair": trading_pair,
                    "trade_type": (
                        float(TradeType.SELL.value)
                        if trade["S"] == OrderSide.SELL.value
                        else float(TradeType.BUY.value)
                    ),
                    "trade_id": trade["i"],
                    "price": float(trade["p"]),
                    "amount": float(trade["v"]),
                },
                timestamp=trade["T"] * 1e-3,
            )

            message_queue.put_nowait(trade_message)

    async def _parse_funding_info_message(
        self, raw_message: Dict[str, Any], message_queue: asyncio.Queue
    ):
        data = raw_message["data"]
        trading_pair = data["symbol"]

        funding_info_update = FundingInfoUpdate(
            trading_pair=trading_pair,
            index_price=Decimal(data["index_price"]),
            mark_price=Decimal(data["mark_price"]),
            next_funding_utc_timestamp=data["next_funding_time"],
            rate=Decimal(data["funding_rate"]),
        )
        message_queue.put_nowait(funding_info_update)

    async def _parse_order_book_snapshot_message(
        self, raw_message: Dict[str, Any], message_queue: asyncio.Queue
    ):
        data = raw_message["data"]
        trading_pair = data["s"]
        bids = [(float(row[0]), float(row[1])) for row in data["b"]]
        asks = [(float(row[0]), float(row[1])) for row in data["a"]]
        order_book_message = OrderBookMessage(
            OrderBookMessageType.SNAPSHOT,
            {
                "trading_pair": trading_pair,
                "update_id": data["seq"],
                "bids": bids,
                "asks": asks,
            },
            timestamp=data["ts"] * 1e-3,
        )
        message_queue.put_nowait(order_book_message)

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
