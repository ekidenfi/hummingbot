import asyncio
import time
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import hummingbot.connector.derivative.ekiden_perpetual.ekiden_perpetual_constants as CONSTANTS
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.funding_info import FundingInfo, FundingInfoUpdate
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.perpetual_api_order_book_data_source import PerpetualAPIOrderBookDataSource
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant

if TYPE_CHECKING:
    from hummingbot.connector.derivative.ekiden_perpetual.ekiden_perpetual_derivative import EkidenPerpetualDerivative


class EkidenPerpetualAPIOrderBookDataSource(PerpetualAPIOrderBookDataSource):
    def __init__(
        self,
        trading_pairs: List[str],
        api_factory: WebAssistantsFactory,
        connector: "EkidenPerpetualDerivative",
        domain: str = CONSTANTS.DOMAIN,
    ):
        super().__init__(trading_pairs)
        self._api_factory = api_factory
        self._connector = connector
        self.domain = domain

    async def get_last_traded_prices(
        self, trading_pairs: List[str]
    ) -> Dict[str, float]:
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    async def _request_order_book_snapshot(self):
        raise NotImplementedError

    async def _order_book_snapshot(self):
        self.logger().warning(
            f"Order book snapshots are not supported for {self._connector.name}"
        )
        raise NotImplementedError

    async def _connected_websocket_assistant(self) -> WSAssistant:
        url = f"{CONSTANTS.PERPETUAL_WS_URL}{CONSTANTS.WS_PUBLIC}"
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        await ws.connect(ws_url=url, ping_timeout=CONSTANTS.HEARTBEAT_TIME_INTERVAL)
        return ws

    async def _subscribe_channels(self, ws: WSAssistant):
        """
        Subscribes to the trade events and diff orders events through the provided websocket connection.

        :param ws: the websocket assistant used to connect to the exchange
        """
        try:
            id = int(time.time())
            for trading_pair in self._trading_pairs:
                market_addr = await self._connector.market_address_associated_to_pair(
                    trading_pair=trading_pair
                )
                order_book_payload = {
                    "op": "subscribe",
                    "args": [f"orderbook/{market_addr}"],
                    "req_id": f"{id}",
                }
                trades_payload = {
                    "op": "subscribe",
                    "args": [f"trade/{market_addr}"],
                    "req_id": f"{id + 1}",
                }
                ticker_payload = {
                    "op": "subscribe",
                    "args": [f"ticker/{market_addr}"],
                    "req_id": f"{id + 2}",
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

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        logger = self.logger()
        channel = ""
        topic: Optional[str] = None

        op = event_message["op"]
        match op:
            case "subscribed":
                args = event_message["args"]
                logger.info(f"Subscribed to {args} successfully")
            case "unsubscribed":
                pass
            case "pong":
                logger.info("Received pong")
            case "error":
                msg = event_message["message"]
                logger.error(f"Error from order book ws_stream: , msg={msg}")
            case "event":
                logger.info("Received event")
                topic: str = event_message["topic"].split("/")[0]
            case "_":
                logger.warning(f"Unrecognized order book ws_stream op: {op}")

        match topic:
            case CONSTANTS.WS_TRADES:
                channel = self._trade_messages_queue_key
            case CONSTANTS.WS_ORDERBOOK:
                channel = self._diff_messages_queue_key
            case CONSTANTS.WS_TICKER:
                channel = self._funding_info_messages_queue_key

        return channel

    async def _parse_order_book_diff_message(
        self, raw_message: Dict[str, Any], message_queue: asyncio.Queue
    ):
        timestamp: float = raw_message["data"]["timestamp"] * 1e-3
        trading_pair = await self._connector.trading_pair_associated_to_market_address(
            raw_message["data"]["market_addr"]
        )
        data = raw_message["data"]
        bids = [(float(row[0]), float(row[1])) for row in data["bids"]]
        asks = [(float(row[0]), float(row[1])) for row in data["asks"]]

        order_book_message: OrderBookMessage = OrderBookMessage(
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

        for trade in trades:
            trade_message: OrderBookMessage = OrderBookMessage(
                OrderBookMessageType.TRADE,
                {
                    "trading_pair": trading_pair,
                    "trade_type": (
                        float(TradeType.SELL.value)
                        if trade["side"] == "sell"
                        else float(TradeType.BUY.value)
                    ),
                    "trade_id": trade["id"],
                    "price": float(trade["price"]),
                    "amount": float(trade["size"]),
                },
                timestamp=trade["timestamp"] * 1e-3,
            )

            message_queue.put_nowait(trade_message)

    async def _parse_funding_info_message(
        self, raw_message: Dict[str, Any], message_queue: asyncio.Queue
    ):
        data = raw_message["data"]
        trading_pair = await self._connector.trading_pair_associated_to_market_address(
            raw_message["market_addr"]
        )

        funding_info_update = FundingInfoUpdate(
            trading_pair=trading_pair,
            index_price=Decimal(data["index_price"]),
            mark_price=Decimal(data["mark_price"]),
            next_funding_utc_timestamp=data["next_funding_time"],
            rate=Decimal(data["funding_rate"]),
        )
        message_queue.put_nowait(funding_info_update)

    async def get_funding_info(self, trading_pair: str) -> FundingInfo:
        symbol = await self._connector.exchange_symbol_associated_to_pair(
            trading_pair=trading_pair
        )
        response: List = await self._request_pair_funding_info(symbol)

        timestamp_str = response[0]["next_funding_time"]
        dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
        unix_ts = dt.timestamp()
        unix_ts_ms = int(unix_ts * 1000)

        funding_info = FundingInfo(
            trading_pair=trading_pair,
            index_price=Decimal(response[0]["oracle_price"]),
            mark_price=Decimal(response[0]["mark_price"]),
            next_funding_utc_timestamp=unix_ts_ms,
            rate=Decimal(response[0]["funding_rate_raw"]),
        )
        return funding_info

    async def _request_pair_funding_info(self, trading_pair: str):
        data = await self._connector._api_get(
            path_url=CONSTANTS.MARKET_FUNDING,
            params={"symbol": trading_pair},
        )
        return data

    def _next_funding_time(self) -> int:
        """
        Funding settlement occurs every 1 hours as mentioned in https://docs.ekiden.fi/api-reference/integration/pnl-margin-fr
        """
        return int(((time.time() // 3600) + 1) * 3600)
