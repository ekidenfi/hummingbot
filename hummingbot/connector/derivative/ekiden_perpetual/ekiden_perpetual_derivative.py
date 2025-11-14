import asyncio
import hashlib
import time
from decimal import Decimal
from typing import Any, AsyncIterable, Dict, List, Optional, Tuple

from bidict import bidict

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.derivative.ekiden_perpetual import (
    ekiden_perpetual_constants as CONSTANTS,
    ekiden_perpetual_signing as ekiden_signing,
    ekiden_perpetual_web_utils as web_utils,
)
from hummingbot.connector.derivative.ekiden_perpetual.ekiden_perpetual_api_order_book_data_source import (
    EkidenPerpetualAPIOrderBookDataSource,
)
from hummingbot.connector.derivative.ekiden_perpetual.ekiden_perpetual_auth import EkidenPerpetualAuth
from hummingbot.connector.derivative.ekiden_perpetual.ekiden_perpetual_user_stream_data_source import (
    EkidenPerpetualUserStreamDataSource,
)
from hummingbot.connector.derivative.position import Position
from hummingbot.connector.perpetual_derivative_py_base import PerpetualDerivativePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair, get_new_client_order_id, split_hb_trading_pair
from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode, PositionSide, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.async_utils import safe_ensure_future, safe_gather
from hummingbot.core.utils.estimate_fee import build_trade_fee
from hummingbot.core.utils.tracking_nonce import NonceCreator
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


class EkidenPerpetualDerivative(PerpetualDerivativePyBase):
    web_utils = web_utils

    def __init__(
        self,
        balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
        domain: str = CONSTANTS.DOMAIN,
        aptos_private_key: str = None,
        rate_limits_share_pct: Decimal = Decimal("100"),
        trading_pairs: Optional[List[str]] = None,
        trading_required: bool = True,
    ):
        self._domain = domain
        self._last_trade_history_timestamp = None
        self._position_mode = None
        self._trading_pairs = trading_pairs
        self._trading_required = trading_required
        self._nonce_provider = NonceCreator.for_seconds()
        self.coin_to_asset: Dict[str, int] = {}
        self.aptos_private_key = aptos_private_key
        super().__init__(balance_asset_limit, rate_limits_share_pct)

    @property
    def name(self) -> str:
        return CONSTANTS.EXCHANGE_NAME

    @property
    def authenticator(self) -> Optional[EkidenPerpetualAuth]:
        return EkidenPerpetualAuth(self.aptos_private_key)

    @property
    def rate_limits_rules(self) -> List[RateLimit]:
        return CONSTANTS.RATE_LIMITS

    @property
    def domain(self) -> str:
        return self._domain

    @property
    def client_order_id_max_length(self) -> int:
        return None

    @property
    def client_order_id_prefix(self) -> str:
        return CONSTANTS.BROKER_ID

    @property
    def trading_rules_request_path(self) -> str:
        return CONSTANTS.MARKET_INFO

    @property
    def trading_pairs_request_path(self) -> str:
        return CONSTANTS.MARKET_INFO

    @property
    def market_addresses_request_path(self) -> str:
        return CONSTANTS.MARKET_INFO

    @property
    def check_network_request_path(self) -> str:
        return CONSTANTS.PING_URL

    @property
    def trading_pairs(self):
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        return True

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    @property
    def funding_fee_poll_interval(self) -> int:
        return 120

    async def _make_network_check_request(self):
        await self._api_get(path_url=self.check_network_request_path)

    def supported_order_types(self) -> List[OrderType]:
        """
        :return a list of OrderType supported by this connector
        """
        return [OrderType.LIMIT, OrderType.MARKET]

    def supported_position_modes(self):
        """
        This method needs to be overridden to provide the accurate information depending on the exchange.
        """
        return [PositionMode.ONEWAY]

    def get_buy_collateral_token(self, trading_pair: str) -> str:
        trading_rule: TradingRule = self._trading_rules[trading_pair]
        return trading_rule.buy_order_collateral_token

    def get_sell_collateral_token(self, trading_pair: str) -> str:
        trading_rule: TradingRule = self._trading_rules[trading_pair]
        return trading_rule.sell_order_collateral_token

    def _is_request_exception_related_to_time_synchronizer(
        self, request_exception: Exception
    ):
        return False

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(throttler=self._throttler, auth=self._auth)

    async def _make_trading_rules_request(self) -> Any:
        exchange_info = await self._api_get(path_url=self.trading_rules_request_path)
        return exchange_info

    async def _make_trading_pairs_request(self) -> Any:
        exchange_info = await self._api_get(path_url=self.trading_pairs_request_path)
        return exchange_info

    async def _make_market_addresses_request(self) -> Any:
        exchange_info = await self._api_get(path_url=self.market_addresses_request_path)
        return exchange_info

    async def market_address_associated_to_pair(self, trading_pair: str) -> str:
        """
        Used to translate a trading pair from the client notation to the exchange market address

        :param trading_pair: trading pair in client notation

        :return: market address
        """
        if getattr(self, "market_addresses_trading_pair_map", None):
            address_map = self.market_addresses_trading_pair_map
            return address_map.inverse[trading_pair]

    async def trading_pair_associated_to_market_address(
        self,
        market_address: str,
    ) -> str:
        """
        Used to translate a trading pair from the exchange market address

        :param market_address: market address from exchange

        :return: trading pair in client notation
        """
        if getattr(self, "market_addresses_trading_pair_map", None):
            address_map = self.market_addresses_trading_pair_map
            return address_map[market_address]

    def _is_order_not_found_during_status_update_error(
        self, status_update_exception, LinkedLimitWeightPair: Exception
    ) -> bool:
        return CONSTANTS.ORDER_NOT_EXIST_MESSAGE in str(status_update_exception)

    def _is_order_not_found_during_cancelation_error(
        self, cancelation_exception: Exception
    ) -> bool:
        return CONSTANTS.UNKNOWN_ORDER_MESSAGE in str(cancelation_exception)

    def quantize_order_price(self, trading_pair: str, price: Decimal) -> Decimal:
        """
        Applies trading rule to quantize order price.
        """
        d_price = Decimal(round(float(f"{price:.5g}"), 6))
        return d_price

    async def _update_trading_rules(self):
        exchange_info = await self._api_get(
            path_url=self.trading_rules_request_path,
        )
        trading_rules_list = await self._format_trading_rules(exchange_info)
        self._trading_rules.clear()
        for trading_rule in trading_rules_list:
            self._trading_rules[trading_rule.trading_pair] = trading_rule
        self._initialize_trading_pair_symbols_from_exchange_info(
            exchange_info=exchange_info
        )
        self._initialize_market_addresses_from_exchange_info(
            exchange_info=exchange_info
        )

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return EkidenPerpetualAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self.domain,
        )

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return EkidenPerpetualUserStreamDataSource(
            auth=self._auth,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self.domain,
        )

    async def _status_polling_loop_fetch_updates(self):
        await safe_gather(
            self._update_trade_history(),
            self._update_order_status(),
            self._update_balances(),
            self._update_positions(),
        )

    async def _update_order_status(self):
        await self._update_orders()

    async def _update_lost_orders_status(self):
        await self._update_lost_orders()

    def _get_fee(
        self,
        base_currency: str,
        quote_currency: str,
        order_type: OrderType,
        order_side: TradeType,
        position_action: PositionAction,
        amount: Decimal,
        price: Decimal = s_decimal_NaN,
        is_maker: Optional[bool] = None,
    ) -> TradeFeeBase:
        is_maker = is_maker or False
        fee = build_trade_fee(
            self.name,
            is_maker,
            base_currency=base_currency,
            quote_currency=quote_currency,
            order_type=order_type,
            order_side=order_side,
            amount=amount,
            price=price,
        )
        return fee

    async def _update_trading_fees(self):
        """
        Update fees information from the exchange
        """
        pass

    # === Orders placing ===

    def buy(
        self,
        trading_pair: str,
        amount: Decimal,
        order_type=OrderType.LIMIT,
        price: Decimal = s_decimal_NaN,
        **kwargs,
    ) -> str:
        """
        Creates a promise to create a buy order using the parameters

        :param trading_pair: the token pair to operate with
        :param amount: the order amount
        :param order_type: the type of order to create (MARKET, LIMIT, LIMIT_MAKER)
        :param price: the order price

        :return: the id assigned by the connector to the order (the client id)
        """
        order_id = get_new_client_order_id(
            is_buy=True,
            trading_pair=trading_pair,
            hbot_order_id_prefix=self.client_order_id_prefix,
            max_id_len=self.client_order_id_max_length,
        )
        md5 = hashlib.md5()
        md5.update(order_id.encode("utf-8"))
        hex_order_id = f"0x{md5.hexdigest()}"
        if order_type is OrderType.MARKET:
            mid_price = self.get_mid_price(trading_pair)
            slippage = CONSTANTS.MARKET_ORDER_SLIPPAGE
            market_price = mid_price * Decimal(1 + slippage)
            price = self.quantize_order_price(trading_pair, market_price)

        safe_ensure_future(
            self._create_order(
                trade_type=TradeType.BUY,
                order_id=hex_order_id,
                trading_pair=trading_pair,
                amount=amount,
                order_type=order_type,
                price=price,
                **kwargs,
            )
        )
        return hex_order_id

    def sell(
        self,
        trading_pair: str,
        amount: Decimal,
        order_type: OrderType = OrderType.LIMIT,
        price: Decimal = s_decimal_NaN,
        **kwargs,
    ) -> str:
        """
        Creates a promise to create a sell order using the parameters.
        :param trading_pair: the token pair to operate with
        :param amount: the order amount
        :param order_type: the type of order to create (MARKET, LIMIT, LIMIT_MAKER)
        :param price: the order price
        :return: the id assigned by the connector to the order (the client id)
        """
        order_id = get_new_client_order_id(
            is_buy=False,
            trading_pair=trading_pair,
            hbot_order_id_prefix=self.client_order_id_prefix,
            max_id_len=self.client_order_id_max_length,
        )
        md5 = hashlib.md5()
        md5.update(order_id.encode("utf-8"))
        hex_order_id = f"0x{md5.hexdigest()}"
        if order_type is OrderType.MARKET:
            mid_price = self.get_mid_price(trading_pair)
            slippage = CONSTANTS.MARKET_ORDER_SLIPPAGE
            market_price = mid_price * Decimal(1 - slippage)
            price = self.quantize_order_price(trading_pair, market_price)

        safe_ensure_future(
            self._create_order(
                trade_type=TradeType.SELL,
                order_id=hex_order_id,
                trading_pair=trading_pair,
                amount=amount,
                order_type=order_type,
                price=price,
                **kwargs,
            )
        )
        return hex_order_id

    async def _place_order(
        self,
        order_id: str,
        trading_pair: str,
        amount: Decimal,
        trade_type: TradeType,
        order_type: OrderType,
        price: Decimal,
        position_action: PositionAction = PositionAction.NIL,
        **kwargs,
    ) -> Tuple[str, float]:
        market_addr = await self.market_address_associated_to_pair(trading_pair)
        is_buy = trade_type is TradeType.BUY
        is_reduce = position_action == PositionAction.CLOSE

        nonce = self._nonce_provider.get_tracking_nonce()

        tif = "GTC"
        if order_type is OrderType.LIMIT:
            tif = "ALO"
        elif order_type is OrderType.MARKET:
            tif = "IOC"

        payload = {
            "type": "order_create",
            "orders": [
                {
                    "market_addr": market_addr,
                    "side": "buy" if is_buy else "sell",
                    "size": int(amount),
                    "price": int(price),
                    "leverage": 1,
                    "type": "limit" if order_type.is_limit_type() else "market",
                    "is_cross": True,
                    "time_in_force": tif,
                    "reduce_only": is_reduce,
                }
            ],
        }

        signature = ekiden_signing.sign_intent(
            self._auth.trading_account, payload, nonce
        )

        data = {
            "payload": payload,
            "nonce": nonce,
            "signature": signature,
            "user_addr": self._auth.trading_address,
        }

        order_result = await self._api_post(
            path_url=CONSTANTS.USER_SEND_INTENT,
            data=data,
            is_auth_required=True,
        )

        try:
            response_data = order_result["response"]["data"]
            statuses = response_data.get("statuses", [])
            if not statuses:
                raise IOError("Unexpected order response: missing statuses")

            status_info = statuses[0]
            if "error" in status_info:
                raise IOError(
                    f"Error submitting order {order_id}: {status_info['error']}"
                )

            o_data = status_info.get("resting") or status_info.get("filled")
            sid = o_data.get("sid") if o_data else None
            if not sid:
                raise IOError("Missing order SID in response")

            self.logger().info(f"Order {order_id} placed successfully (SID={sid})")
            return sid, self.current_timestamp

        except Exception as e:
            self.logger().exception(f"Order placement failed: {e}")
            raise IOError(f"Order placement failed: {e}")

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder) -> bool:
        nonce = self._nonce_provider.get_tracking_nonce()

        payload = {
            "type": "order_cancel",
            "cancels": [{"sid": order_id}],
        }

        signature = ekiden_signing.sign_intent(
            self._auth.trading_account, payload, nonce
        )

        data = {
            "payload": payload,
            "nonce": nonce,
            "signature": signature,
            "user_addr": self._auth.trading_address,
        }

        cancel_result = await self._api_post(
            path_url=CONSTANTS.USER_SEND_INTENT,
            data=data,
            is_auth_required=True,
        )

        try:
            response_data = cancel_result["response"]["data"]
            statuses = response_data.get("statuses", [])
            if not statuses:
                raise IOError("Unexpected cancel response format: missing statuses")

            status_info = statuses[0]
            if "error" in status_info:
                error_msg = status_info["error"]
                self.logger().debug(f"Cancel failed: {error_msg}")
                await self._order_tracker.process_order_not_found(order_id)
                raise IOError(error_msg)

            if "success" in status_info:
                self.logger().info(f"Order {order_id} cancelled successfully.")
                return True

        except Exception as e:
            self.logger().exception(f"Error parsing cancel response: {e}")
            raise IOError(f"Cancel request failed: {e}")

        return False

    async def _update_trade_history(self):
        orders = list(self._order_tracker.all_fillable_orders.values())
        all_fillable_orders = (
            self._order_tracker.all_fillable_orders_by_exchange_order_id
        )
        all_fills_response = []
        if len(orders) > 0:
            try:
                all_fills_response = await self._api_get(
                    path_url=CONSTANTS.ACCOUNT_TRADE_LIST_URL,
                    data={
                        "type": CONSTANTS.TRADES_TYPE,
                        "user": self.ekiden_perpetual_api_key,
                    },
                )
            except asyncio.CancelledError:
                raise
            except Exception as request_error:
                self.logger().warning(
                    f"Failed to fetch trade updates. Error: {request_error}",
                    exc_info=request_error,
                )
            for trade_fill in all_fills_response:
                self._process_trade_rs_event_message(
                    order_fill=trade_fill, all_fillable_order=all_fillable_orders
                )

    def _process_trade_rs_event_message(
        self, order_fill: Dict[str, Any], all_fillable_order
    ):
        exchange_order_id = str(order_fill.get("oid"))
        fillable_order = all_fillable_order.get(exchange_order_id)
        if fillable_order is not None:
            fee_asset = fillable_order.quote_asset

            position_action = (
                PositionAction.OPEN
                if order_fill["dir"].split(" ")[0] == "Open"
                else PositionAction.CLOSE
            )
            fee = TradeFeeBase.new_perpetual_fee(
                fee_schema=self.trade_fee_schema(),
                position_action=position_action,
                percent_token=fee_asset,
                flat_fees=[
                    TokenAmount(amount=Decimal(order_fill["fee"]), token=fee_asset)
                ],
            )

            trade_update = TradeUpdate(
                trade_id=str(order_fill["tid"]),
                client_order_id=fillable_order.client_order_id,
                exchange_order_id=str(order_fill["oid"]),
                trading_pair=fillable_order.trading_pair,
                fee=fee,
                fill_base_amount=Decimal(order_fill["sz"]),
                fill_quote_amount=Decimal(order_fill["px"]) * Decimal(order_fill["sz"]),
                fill_price=Decimal(order_fill["px"]),
                fill_timestamp=order_fill["time"] * 1e-3,
            )

            self._order_tracker.process_trade_update(trade_update)

    async def _all_trade_updates_for_order(
        self, order: InFlightOrder
    ) -> List[TradeUpdate]:
        pass

    async def _handle_update_error_for_active_order(
        self, order: InFlightOrder, error: Exception
    ):
        try:
            raise error
        except (asyncio.TimeoutError, KeyError):
            self.logger().debug(
                f"Tracked order {order.client_order_id} does not have an exchange id. "
                f"Attempting fetch in next polling interval."
            )
            await self._order_tracker.process_order_not_found(order.client_order_id)
        except asyncio.CancelledError:
            raise
        except Exception as request_error:
            self.logger().warning(
                f"Error fetching status update for the active order {order.client_order_id}: {request_error}.",
            )
            self.logger().debug(
                f"Order {order.client_order_id} not found counter: {self._order_tracker._order_not_found_records.get(order.client_order_id, 0)}"
            )
            await self._order_tracker.process_order_not_found(order.client_order_id)

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        client_order_id = tracked_order.client_order_id
        trading_pair = tracked_order.trading_pair
        side = tracked_order.trade_type.name.lower()

        try:
            exchange_order_id = (
                tracked_order.exchange_order_id
                or await tracked_order.get_exchange_order_id()
            )
        except asyncio.TimeoutError:
            exchange_order_id = None

        market_addr = self._market_addr_for_pair.get(trading_pair)

        query_params = {
            "market_addr": market_addr,
            "side": side,
            "page": 1,
            "per_page": 50,  # fetch enough to find our order
        }

        orders = await self._api_get(
            path_url=CONSTANTS.USER_ORDERS,
            params=query_params,
            is_auth_required=True,
        )

        if not isinstance(orders, list):
            self.logger().error(f"Unexpected orders response: {orders}")
            raise ValueError("Invalid response format from Ekiden user/orders")

        matched_order = None
        for order in orders:
            if (
                str(order.get("seq")) == str(exchange_order_id)
                or order.get("sid") == client_order_id
            ):
                matched_order = order
                break

        if not matched_order:
            raise ValueError(
                f"Order {exchange_order_id or client_order_id} not found in user/orders response"
            )

        order_state = matched_order["status"].lower()
        order_update = OrderUpdate(
            trading_pair=trading_pair,
            update_timestamp=matched_order["timestamp_ms"] / 1e3,
            new_state=CONSTANTS.ORDER_STATUSES[order_state],
            client_order_id=client_order_id,
            exchange_order_id=str(matched_order["seq"]),
        )

        return order_update

    async def _iter_user_event_queue(self) -> AsyncIterable[Dict[str, any]]:
        while True:
            try:
                yield await self._user_stream_tracker.user_stream.get()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network(
                    "Unknown error. Retrying after 1 seconds.",
                    exc_info=True,
                    app_warning_msg="Could not fetch user events from Ekiden. Check API key and network connection.",
                )
                await self._sleep(1.0)

    async def _user_stream_event_listener(self):
        async for event_message in self._iter_user_event_queue():
            try:
                if isinstance(event_message, dict):
                    channel: str = event_message.get("channel", None)
                    results = event_message.get("data", None)
                elif event_message is asyncio.CancelledError:
                    raise asyncio.CancelledError
                else:
                    raise Exception(event_message)
                if channel not in CONSTANTS.PRIVATE_TOPICS:
                    self.logger().error(
                        f"Unexpected message in user stream: {event_message}.",
                        exc_info=True,
                    )
                    continue

                match channel:
                    case CONSTANTS.WS_USER_ORDER:
                        for order_msg in results:
                            self._process_order_message(order_msg)
                    case CONSTANTS.WS_USER_FILL:
                        for trade_msg in results["fills"]:
                            await self._process_trade_message(trade_msg)
                    case CONSTANTS.WS_USER_POSITION:
                        ...

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error(
                    "Unexpected error in user stream listener loop.", exc_info=True
                )
                await self._sleep(5.0)

    async def _process_trade_message(
        self, trade: Dict[str, Any], client_order_id: Optional[str] = None
    ):
        """
        Updates in-flight order and trigger order filled event for trade message received. Triggers order completed
        event if the total executed amount equals to the specified order amount.
        Example Trade:
        """
        exchange_order_id = str(trade.get("oid", ""))
        tracked_order = (
            self._order_tracker.all_fillable_orders_by_exchange_order_id.get(
                exchange_order_id
            )
        )

        if tracked_order is None:
            all_orders = self._order_tracker.all_fillable_orders
            for k, v in all_orders.items():
                await v.get_exchange_order_id()
            _cli_tracked_orders = [
                o
                for o in all_orders.values()
                if exchange_order_id == o.exchange_order_id
            ]
            if not _cli_tracked_orders:
                self.logger().debug(
                    f"Ignoring trade message with id {client_order_id}: not in in_flight_orders."
                )
                return
            tracked_order = _cli_tracked_orders[0]
        trading_pair_base_coin = tracked_order.base_asset
        if trade["coin"] == trading_pair_base_coin:
            position_action = (
                PositionAction.OPEN
                if trade["dir"].split(" ")[0] == "Open"
                else PositionAction.CLOSE
            )
            fee_asset = tracked_order.quote_asset
            fee = TradeFeeBase.new_perpetual_fee(
                fee_schema=self.trade_fee_schema(),
                position_action=position_action,
                percent_token=fee_asset,
                flat_fees=[TokenAmount(amount=Decimal(trade["fee"]), token=fee_asset)],
            )
            trade_update: TradeUpdate = TradeUpdate(
                trade_id=str(trade["tid"]),
                client_order_id=tracked_order.client_order_id,
                exchange_order_id=str(trade["oid"]),
                trading_pair=tracked_order.trading_pair,
                fill_timestamp=trade["time"] * 1e-3,
                fill_price=Decimal(trade["px"]),
                fill_base_amount=Decimal(trade["sz"]),
                fill_quote_amount=Decimal(trade["px"]) * Decimal(trade["sz"]),
                fee=fee,
            )
            self._order_tracker.process_trade_update(trade_update)

    def _process_order_message(self, order_msg: Dict[str, Any]):
        """
        Updates in-flight order and triggers cancelation or failure event if needed.

        :param order_msg: The order response from either REST or web socket API (they are of the same format)

        Example Order:
        """
        client_order_id = str(order_msg["order"].get("cloid", ""))
        tracked_order = self._order_tracker.all_updatable_orders.get(client_order_id)
        if not tracked_order:
            self.logger().debug(
                f"Ignoring order message with id {client_order_id}: not in in_flight_orders."
            )
            return
        current_state = order_msg["status"]
        tracked_order.update_exchange_order_id(str(order_msg["order"]["oid"]))
        order_update: OrderUpdate = OrderUpdate(
            trading_pair=tracked_order.trading_pair,
            update_timestamp=order_msg["statusTimestamp"] * 1e-3,
            new_state=CONSTANTS.ORDER_STATE[current_state],
            client_order_id=order_msg["order"]["cloid"],
            exchange_order_id=str(order_msg["order"]["oid"]),
        )
        self._order_tracker.process_order_update(order_update=order_update)

    async def _format_trading_rules(
        self, exchange_info_list: List[Dict]
    ) -> List[TradingRule]:
        trading_rules: List[TradingRule] = []

        try:
            for market in exchange_info_list:
                symbol = market.get("symbol")
                if not symbol:
                    self.logger().warning(
                        f"Skipping market with missing symbol: {market}"
                    )
                    continue
                trading_pair = await self.trading_pair_associated_to_exchange_symbol(
                    symbol=symbol
                )
                base_decimals = int(market.get("base_decimals", 0))
                quote_decimals = int(market.get("quote_decimals", 0))
                min_order_size = Decimal(str(market.get("min_order_size", 1))) / (
                    Decimal(10) ** base_decimals
                )
                min_price_increment = Decimal(10) ** (-quote_decimals)
                min_base_amount_increment = Decimal(10) ** (-base_decimals)
                collateral_token = split_hb_trading_pair(trading_pair)[1]
                rule = TradingRule(
                    trading_pair=trading_pair,
                    min_order_size=min_order_size,
                    min_base_amount_increment=min_base_amount_increment,
                    min_price_increment=min_price_increment,
                    buy_order_collateral_token=collateral_token,
                    sell_order_collateral_token=collateral_token,
                )
                trading_rules.append(rule)
            self.logger().debug(
                f"Loaded {len(trading_rules)} trading rules from Ekiden API."
            )
        except Exception as e:
            self.logger().exception(f"Error parsing trading rules: {e}")
            raise
        return trading_rules

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: List):
        mapping = bidict()
        for symbol_data in exchange_info:
            exchange_symbol = symbol_data["symbol"]
            base, quote = exchange_symbol.split("-")
            trading_pair = combine_to_hb_trading_pair(base, quote)
            if trading_pair in mapping.inverse:
                self._resolve_trading_pair_symbols_duplicate(
                    mapping, exchange_symbol, base, quote
                )
            else:
                mapping[exchange_symbol] = trading_pair
        self._set_trading_pair_symbol_map(mapping)

    def _initialize_market_addresses_from_exchange_info(self, exchange_info: List):
        mapping = bidict()
        for symbol_data in exchange_info:
            exchange_symbol = symbol_data["symbol"]
            market_address = symbol_data["addr"]
            base, quote = exchange_symbol.split("-")
            trading_pair = combine_to_hb_trading_pair(base, quote)
            if trading_pair in mapping.inverse:
                self._resolve_trading_pair_symbols_duplicate(
                    mapping, exchange_symbol, base, quote
                )
            mapping[market_address] = trading_pair
        self.market_addresses_trading_pair_map = mapping

    def _resolve_trading_pair_symbols_duplicate(
        self, mapping: bidict, new_exchange_symbol: str, base: str, quote: str
    ):
        """Resolves name conflicts provoked by futures contracts.

        If the expected BASEQUOTE combination matches one of the exchange symbols, it is the one taken, otherwise,
        the trading pair is removed from the map and an error is logged.
        """
        expected_exchange_symbol = f"{base}{quote}"
        trading_pair = combine_to_hb_trading_pair(base, quote)
        current_exchange_symbol = mapping.inverse[trading_pair]
        if current_exchange_symbol == expected_exchange_symbol:
            pass
        elif new_exchange_symbol == expected_exchange_symbol:
            mapping.pop(current_exchange_symbol)
            mapping[new_exchange_symbol] = trading_pair
        else:
            self.logger().error(
                f"Could not resolve the exchange symbols {new_exchange_symbol} and {current_exchange_symbol}"
            )
            mapping.pop(current_exchange_symbol)

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        market_addr = await self.market_address_associated_to_pair(trading_pair)

        response = await self._api_get(
            path_url=f"{CONSTANTS.MARKET_CANDLES_STATS}/{market_addr}",
        )

        return float(response.get("current_price", 0.0))

    async def _update_balances(self):
        """
        Calls the REST API to update total and available balances.
        """

        account_info = await self._api_get(
            path_url=CONSTANTS.USER_PORTFOLIO,
            is_auth_required=True,
        )

        quote = CONSTANTS.CURRENCY
        summary: Dict[str, Any] = account_info["summary"]
        total_balance: str = summary.get("total_balance", "0")
        available_balance: str = summary.get("total_available_balance", "0")

        self._account_balances[quote] = (
            Decimal(total_balance) / CONSTANTS.CURRENCY_DECIMALS
        )
        self._account_available_balances[quote] = (
            Decimal(available_balance) / CONSTANTS.CURRENCY_DECIMALS
        )

    async def _update_positions(self):
        positions = await self._api_get(
            path_url=CONSTANTS.USER_POSITIONS,
            is_auth_required=True,
        )

        if not positions:
            for key in list(self._perpetual_trading.account_positions.keys()):
                self._perpetual_trading.remove_position(key)
            return

        for pos in positions:
            market_addr = pos.get("market_addr")
            trading_pair = self.trading_pair_associated_to_market_address(market_addr)
            market_info = self._trading_rules.get(trading_pair, None)
            if not market_info:
                raise IOError("Missing the market info for the active position")

            ex_trading_pair = market_info["symbol"]
            hb_trading_pair = await self.trading_pair_associated_to_exchange_symbol(
                ex_trading_pair
            )

            position_side = (
                PositionSide.LONG if pos.get("side") == "long" else PositionSide.SHORT
            )

            amount = abs(Decimal(pos.get("size", 0)))
            entry_price = Decimal(pos.get("entry_price", 0))
            unrealized_pnl = Decimal(pos.get("unrealized_pnl", 0))
            leverage = Decimal(pos.get("leverage", 1))

            pos_key = self._perpetual_trading.position_key(
                hb_trading_pair, position_side
            )

            if amount > 0:
                _position = Position(
                    trading_pair=hb_trading_pair,
                    position_side=position_side,
                    unrealized_pnl=unrealized_pnl,
                    entry_price=entry_price,
                    amount=amount,
                    leverage=leverage,
                )
                self._perpetual_trading.set_position(pos_key, _position)
            else:
                self._perpetual_trading.remove_position(pos_key)

    async def _get_position_mode(self) -> Optional[PositionMode]:
        return PositionMode.ONEWAY

    async def _trading_pair_position_mode_set(
        self, mode: PositionMode, trading_pair: str
    ) -> Tuple[bool, str]:
        msg = ""
        success = True
        initial_mode = await self._get_position_mode()
        if initial_mode != mode:
            msg = "ekiden only supports the ONEWAY position mode."
            success = False
        return success, msg

    async def _set_trading_pair_leverage(
        self, trading_pair: str, leverage: int
    ) -> Tuple[bool, str]:
        await self._update_trading_rules()
        market_addr = await self.market_address_associated_to_pair(trading_pair)
        nonce = self._nonce_provider.get_tracking_nonce()
        payload = {
            "type": "leverage_assign",
            "market_addr": market_addr,
            "leverage": leverage,
        }

        signature = ekiden_signing.sign_intent(
            self._auth.trading_account, payload, nonce
        )
        data = {
            "payload": payload,
            "nonce": nonce,
            "signature": signature,
            "user_addr": self._auth.trading_address,
        }
        try:
            result = await self._api_post(
                path_url=CONSTANTS.USER_SEND_INTENT,
                data=data,
                is_auth_required=True,
            )
            statuses = result.get("response", {}).get("data", {}).get("statuses", [])
            if not statuses:
                return False, "No status returned from leverage assignment"
            status_info = statuses[0]
            if "error" in status_info:
                return False, status_info["error"]
            if "success" in status_info:
                return True, "Leverage assigned successfully"
            return False, "Unknown response from leverage assignment"
        except Exception as e:
            return False, f"Error assigning leverage for {trading_pair}: {e}"

    async def _fetch_last_fee_payment(
        self, trading_pair: str
    ) -> Tuple[int, Decimal, Decimal]:
        market_addr = await self.market_address_associated_to_pair(trading_pair)

        positions = await self._api_get(
            path_url=CONSTANTS.USER_POSITIONS, params={"market_addr": market_addr}
        )

        if not positions:
            return 0, Decimal("-1"), Decimal("-1")

        position = positions[0]

        current_funding_index = Decimal(position.get("funding_index", 0))
        entry_funding_index = Decimal(
            position.get("entry_funding_index", current_funding_index)
        )

        size = Decimal(position.get("size", 0))
        side = position.get("side", "buy")

        funding_index_diff = current_funding_index - entry_funding_index

        multiplier = Decimal(1) if side == "buy" else Decimal(-1)

        payment = size * funding_index_diff * multiplier
        funding_rate = funding_index_diff * multiplier

        timestamp = int(position.get("timestamp_ms", 0))

        if payment == Decimal("0"):
            return 0, Decimal("-1"), Decimal("-1")

        return timestamp, funding_rate, payment

    def _last_funding_time(self) -> int:
        """
        Funding settlement occurs every 1 hour as mentioned in https://ekiden.gitbook.io/ekiden-docs/trading/funding
        """
        return int(((time.time() // 3600) - 1) * 3600 * 1e3)
