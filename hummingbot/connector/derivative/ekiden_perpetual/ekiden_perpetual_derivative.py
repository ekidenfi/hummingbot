import asyncio
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from bidict import bidict

import hummingbot.connector.derivative.ekiden_perpetual.ekiden_perpetual_constants as CONSTANTS
from hummingbot.connector.derivative.ekiden_perpetual import (
    ekiden_perpetual_signing as ekiden_signing,
    ekiden_perpetual_web_utils as web_utils,
)
from hummingbot.connector.derivative.ekiden_perpetual.ekiden_perpetual_api_order_book_data_source import (
    EkidenPerpetualAPIOrderBookDataSource,
)
from hummingbot.connector.derivative.ekiden_perpetual.ekiden_perpetual_auth import EkidenPerpetualAuth
from hummingbot.connector.derivative.ekiden_perpetual.ekiden_perpetual_constants import (
    IntentType,
    OrderSide,
    OrderTypeString,
    TimeInForce,
)
from hummingbot.connector.derivative.ekiden_perpetual.ekiden_perpetual_user_stream_data_source import (
    EkidenPerpetualUserStreamDataSource,
)
from hummingbot.connector.derivative.ekiden_perpetual.ekiden_perpetual_utils import get_scale_factors
from hummingbot.connector.derivative.position import Position
from hummingbot.connector.perpetual_derivative_py_base import PerpetualDerivativePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair, split_hb_trading_pair
from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode, PositionSide, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.perpetual_api_order_book_data_source import PerpetualAPIOrderBookDataSource
from hummingbot.core.data_type.trade_fee import TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.async_utils import safe_ensure_future, safe_gather
from hummingbot.core.utils.estimate_fee import build_trade_fee
from hummingbot.core.utils.tracking_nonce import NonceCreator
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

s_decimal_NaN = Decimal("nan")
s_decimal_0 = Decimal(0)


class EkidenPerpetualDerivative(PerpetualDerivativePyBase):
    web_utils = web_utils

    def __init__(
        self,
        balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
        rate_limits_share_pct: Decimal = Decimal("100"),
        aptos_private_key: str = "",
        trading_pairs: Optional[List[str]] = None,
        trading_required: bool = True,
        domain: str = CONSTANTS.DOMAIN,
    ):
        self._aptos_private_key = aptos_private_key
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        self._domain = domain
        self._last_trade_history_timestamp = None
        self._nonce_provider = NonceCreator.for_microseconds()
        self._initialized_rules = False
        super().__init__(balance_asset_limit, rate_limits_share_pct)

    @property
    def name(self) -> str:
        return CONSTANTS.EXCHANGE_NAME

    @property
    def authenticator(self) -> EkidenPerpetualAuth:
        return EkidenPerpetualAuth(self._aptos_private_key, self._trading_required)

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

    async def _make_trading_rules_request(self) -> List[Dict[str, Any]]:
        exchange_info: List[Dict[str, Any]] = await self._api_get(
            path_url=self.trading_rules_request_path
        )
        return exchange_info

    async def _make_trading_pairs_request(self) -> List[Dict[str, Any]]:
        exchange_info: List[Dict[str, Any]] = await self._api_get(
            path_url=self.trading_pairs_request_path
        )
        return exchange_info

    async def _make_network_check_request(self):
        overwrite_url = CONSTANTS.PERPETUAL_BASE_URL + CONSTANTS.PING_URL

        await self._api_get(
            path_url=CONSTANTS.PING_URL,
            overwrite_url=overwrite_url,
        )

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
    ) -> Optional[str]:
        """
        Used to translate a trading pair from the exchange market address

        :param market_address: market address from exchange

        :return: trading pair in client notation
        """
        if getattr(self, "market_addresses_trading_pair_map", None):
            address_map = self.market_addresses_trading_pair_map
            return address_map[market_address]

    @property
    def check_network_request_path(self) -> str:
        return CONSTANTS.PING_URL

    @property
    def trading_pairs(self):
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        return False

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    @property
    def funding_fee_poll_interval(self) -> int:
        return 120

    def supported_order_types(self) -> List[OrderType]:
        """
        :return a list of OrderType supported by this connector
        """
        return [OrderType.LIMIT, OrderType.MARKET]

    def supported_position_modes(self) -> List[PositionMode]:
        return [PositionMode.ONEWAY]

    def get_buy_collateral_token(self, trading_pair: str) -> str:
        trading_rule: TradingRule = self._trading_rules[trading_pair]
        return trading_rule.buy_order_collateral_token

    def get_sell_collateral_token(self, trading_pair: str) -> str:
        trading_rule: TradingRule = self._trading_rules[trading_pair]
        return trading_rule.sell_order_collateral_token

    async def start_network(self):
        await self._update_trading_rules()
        await super().start_network()

    async def _update_trading_rules(self):
        exchange_info: List[Dict[str, Any]] = await self._make_trading_rules_request()
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
        self._initialized_rules = True

    def _is_request_exception_related_to_time_synchronizer(
        self, request_exception: Exception
    ):
        return False

    def _is_order_not_found_during_status_update_error(
        self, status_update_exception: Exception
    ) -> bool:
        return False

    def _is_order_not_found_during_cancelation_error(
        self, cancelation_exception: Exception
    ) -> bool:
        return False

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder) -> bool:
        nonce = self._nonce_provider.get_tracking_nonce()
        payload = {
            "type": IntentType.ORDER_CANCEL.value,
            "cancels": [{"sid": tracked_order.exchange_order_id}],
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
            output = cancel_result.get("output")
            if not output or "outputs" not in output or not output["outputs"]:
                raise IOError(f"Unexpected cancel response format: {cancel_result}")
            sid = output["outputs"][0].get("sid")
            if not sid:
                self.logger().debug(
                    f"Cancel failed for order {order_id}: no SID returned"
                )
                await self._order_tracker.process_order_not_found(order_id)
                raise IOError(f"Cancel failed: no SID returned for order {order_id}")
            self.logger().info(f"Order {order_id} cancelled successfully (SID={sid})")
            return True
        except Exception as e:
            if self.is_order_not_active(e):
                return True
            self.logger().exception(f"Error parsing cancel response: {e}")
            raise IOError(f"Cancel request failed: {e}")

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
        tif = TimeInForce.GTC
        if order_type is OrderType.MARKET:
            tif = TimeInForce.IOC
        o_type = (
            OrderTypeString.LIMIT.value
            if order_type.is_limit_type()
            else OrderTypeString.MARKET.value
        )
        trading_rule = self.trading_rules[trading_pair]
        price_factor, amount_factor = get_scale_factors(trading_rule)
        payload = {
            "type": IntentType.ORDER_CREATE.value,
            "orders": [
                {
                    "is_cross": True,
                    "leverage": 1,
                    "market_addr": market_addr,
                    "order-link-id": order_id,
                    "price": int(price * price_factor),
                    "reduce_only": is_reduce,
                    "side": OrderSide.BUY.value if is_buy else OrderSide.SELL.value,
                    "size": int(amount * amount_factor),
                    "time_in_force": tif.value,
                    "type": o_type,
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
            output = order_result.get("output")
            if not output or "outputs" not in output or not output["outputs"]:
                raise IOError(f"Unexpected order response: {order_result}")
            sid = output["outputs"][0].get("sid")
            if not sid:
                raise IOError(f"Missing order SID in response: {order_result}")
            self.logger().info(f"Order {order_id} placed successfully (SID={sid})")
            return sid, self.current_timestamp
        except Exception as e:
            self.logger().exception(f"Order placement failed: {e}")
            raise IOError(f"Order placement failed: {e}")

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
        pass

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(
            throttler=self._throttler,
            auth=self._auth,
        )

    def _create_order_book_data_source(self) -> PerpetualAPIOrderBookDataSource:
        return EkidenPerpetualAPIOrderBookDataSource(
            self.trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self._domain,
        )

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return EkidenPerpetualUserStreamDataSource(
            auth=self._auth,
            api_factory=self._web_assistants_factory,
            domain=self._domain,
        )

    async def _status_polling_loop_fetch_updates(self):
        await safe_gather(
            self._update_trade_history(),
            self._update_order_status(),
            self._update_balances(),
            self._update_positions(),
        )

    async def _update_trade_history(self):
        orders = list(self._order_tracker.all_fillable_orders.values())
        if len(orders) > 0:
            try:
                all_fills_response: List[Dict[str, Any]] = await self._api_get(
                    path_url=CONSTANTS.USER_FILLS, is_auth_required=True
                )
                for trade_fill in all_fills_response:
                    self._process_trade_message(trade_fill)
            except asyncio.CancelledError:
                raise
            except Exception as request_error:
                self.logger().warning(
                    f"Failed to fetch trade updates. Error: {request_error}",
                    exc_info=request_error,
                )

    async def _update_order_status(self):
        """
        Calls REST API to get order status
        """

        active_orders_ids: List[str] = [
            order.exchange_order_id
            for order in self.in_flight_orders.values()
            if order.exchange_order_id
        ]
        exchange_orders: List[Dict[str, Any]] = await self._api_get(
            path_url=CONSTANTS.USER_ORDERS, is_auth_required=True
        )
        valid_orders: List[Dict[str, Any]] = [
            order for order in exchange_orders if order["sid"] in active_orders_ids
        ]

        for order_data in valid_orders:
            self._process_order_update(order_data)

    def _process_order_update(self, order_data: Dict[str, Any]):
        """
        Updates in-flight order and triggers cancellation or failure event if needed.
        :param order_msg: The order event message payload
        """
        order_status = CONSTANTS.ORDER_STATUSES[order_data["status"]]
        exch_order_id = order_data["sid"]
        client_order_id = order_data.get("order-link-id", None)
        if client_order_id:
            updatable_order = self._order_tracker.all_updatable_orders.get(
                client_order_id
            )
        else:
            updatable_order = (
                self._order_tracker.all_fillable_orders_by_exchange_order_id.get(
                    exch_order_id
                )
            )

        if updatable_order is not None:
            new_order_update: OrderUpdate = OrderUpdate(
                trading_pair=updatable_order.trading_pair,
                update_timestamp=self.current_timestamp,
                new_state=order_status,
                client_order_id=client_order_id,
                exchange_order_id=exch_order_id,
            )
            self._order_tracker.process_order_update(new_order_update)

    async def _update_balances(self):
        """
        Calls REST API to update total and available balances
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
        """
        Retrieves all positions using the REST API.
        """
        positions: List[Dict[str, Any]] = await self._api_get(
            path_url=CONSTANTS.USER_POSITIONS,
            is_auth_required=True,
        )
        if not positions:
            for key in list(self._perpetual_trading.account_positions.keys()):
                self._perpetual_trading.remove_position(key)
            return
        else:
            await self._parse_positions(positions)

    async def _parse_positions(self, positions: List[Dict[str, Any]]):
        for pos in positions:
            market_addr = pos.get("market_addr", "")
            trading_pair = await self.trading_pair_associated_to_market_address(
                market_addr
            )
            if not trading_pair:
                raise IOError(
                    f"No known traiding pair for market address {market_addr}"
                )
            trading_rule = self.trading_rules[trading_pair]
            price_factor, amount_factor = get_scale_factors(trading_rule, inverse=True)
            position_side = (
                PositionSide.LONG if pos.get("side") == "long" else PositionSide.SHORT
            )
            amount = pos.get("size", 0) * amount_factor
            entry_price = pos.get("entry_price", 0) * price_factor
            unrealized_pnl = pos.get("unrealized_pnl", 0) * price_factor
            leverage = pos.get("leverage", 1)
            pos_key = self._perpetual_trading.position_key(trading_pair, position_side)
            if amount > 0:
                _position = Position(
                    trading_pair=trading_pair,
                    position_side=position_side,
                    unrealized_pnl=unrealized_pnl,
                    entry_price=entry_price,
                    amount=amount,
                    leverage=Decimal(leverage if leverage else 1),
                )
                self._perpetual_trading.set_position(pos_key, _position)
            else:
                self._perpetual_trading.remove_position(pos_key)

    async def _all_trade_updates_for_order(
        self, order: InFlightOrder
    ) -> List[TradeUpdate]:
        return []  # Not implemented on ekiden

    async def _request_order_fills(self, order: InFlightOrder) -> Dict[str, Any]:
        pass  # Unused

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        exch_order = await self._request_order_status_data(tracked_order)
        if exch_order:
            exch_status = exch_order["status"].lower()
            order_state = CONSTANTS.ORDER_STATUSES.get(exch_status)
            return OrderUpdate(
                trading_pair=tracked_order.trading_pair,
                update_timestamp=exch_order["timestamp_ms"] / 1e3,
                new_state=order_state if order_state else tracked_order.current_state,
                client_order_id=tracked_order.client_order_id,
                exchange_order_id=exch_order["sid"],
            )
        else:
            return OrderUpdate(
                client_order_id=tracked_order.client_order_id,
                trading_pair=tracked_order.trading_pair,
                update_timestamp=self.current_timestamp,
                new_state=tracked_order.current_state,
            )

    async def _request_order_status_data(
        self, tracked_order: InFlightOrder
    ) -> Dict[str, Any] | None:
        exchange_orders: List[Dict[str, Any]] = await self._api_get(
            path_url=CONSTANTS.USER_ORDERS,
            is_auth_required=True,
        )
        if not tracked_order.exchange_order_id:
            return None
        else:
            return next(
                (
                    order
                    for order in exchange_orders
                    if order["sid"] == tracked_order.exchange_order_id
                ),
                None,
            )

    async def _user_stream_event_listener(self):
        """
        Listens to message in _user_stream_tracker.user_stream queue.
        """
        async for event_message in self._iter_user_event_queue():
            try:
                topic: Optional[str] = event_message.get("topic", None)
                results = event_message.get("data", [])
                if topic not in CONSTANTS.PRIVATE_TOPICS:
                    self.logger().warning(
                        f"Unexpected message in user stream: {event_message}.",
                        exc_info=True,
                    )
                    continue
                match topic:
                    case CONSTANTS.WS_USER_ORDER:
                        for order_msg in results:
                            self._process_order_message(order_msg)
                    case CONSTANTS.WS_USER_FILL:
                        for trade_msg in results["fills"]:
                            self._process_trade_message(trade_msg)
                    case CONSTANTS.WS_USER_POSITION:
                        for position_msg in results:
                            await self._process_position_message(position_msg)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error(
                    "Unexpected error in user stream listener loop.", exc_info=True
                )
                await self._sleep(5.0)

    def _process_order_message(self, order_msg: Dict[str, Any]):
        """
        Updates in-flight order and triggers cancellation or failure event if needed.
        :param order_msg: The order event message payload
        """
        order_status = CONSTANTS.ORDER_STATUSES[order_msg["status"]]
        exch_order_id = order_msg["sid"]
        client_order_id = order_msg.get("order-link-id", None)
        if client_order_id:
            updatable_order = self._order_tracker.all_updatable_orders.get(
                client_order_id
            )
        else:
            updatable_order = (
                self._order_tracker.all_fillable_orders_by_exchange_order_id.get(
                    exch_order_id
                )
            )

        if updatable_order is not None:
            new_order_update: OrderUpdate = OrderUpdate(
                trading_pair=updatable_order.trading_pair,
                update_timestamp=self.current_timestamp,
                new_state=order_status,
                client_order_id=client_order_id,
                exchange_order_id=exch_order_id,
            )
            self._order_tracker.process_order_update(new_order_update)

    def _process_trade_message(self, trade_msg: Dict[str, Any]) -> None:
        """
        Update in-flight order and trigger order filled event for a received trade message.
        Triggers order completed event if the total executed amount equals the specified order amount.

        :param trade_msg: The trade event message payload
        """

        client_order_id = trade_msg.get("order-link-id")
        fillable_order = None

        if client_order_id:
            fillable_order = self._order_tracker.all_fillable_orders.get(
                client_order_id
            )

        if fillable_order is None:
            for sid in [
                trade_msg.get("taker_order_sid"),
                trade_msg.get("maker_order_sid"),
            ]:
                if sid in self._order_tracker.all_fillable_orders_by_exchange_order_id:
                    fillable_order = (
                        self._order_tracker.all_fillable_orders_by_exchange_order_id[
                            sid
                        ]
                    )
                    break

        if fillable_order is None:
            return

        trade_update = self._parse_trade_update(
            trade_msg=trade_msg, tracked_order=fillable_order
        )
        self._order_tracker.process_trade_update(trade_update)

    def _parse_trade_update(
        self, trade_msg: Dict, tracked_order: InFlightOrder
    ) -> TradeUpdate:
        trading_rule = self.trading_rules[tracked_order.trading_pair]
        price_factor, amount_factor = get_scale_factors(trading_rule, inverse=True)
        trade_id = trade_msg["sid"]
        is_maker = tracked_order.exchange_order_id == trade_msg.get("maker_order_sid")
        fee_asset = tracked_order.quote_asset
        fee = trade_msg["maker_fee"] if is_maker else trade_msg["taker_fee"]
        fee_amount = fee * price_factor
        position_side = trade_msg["side"]
        position_action = (
            PositionAction.OPEN
            if (
                (
                    tracked_order.trade_type is TradeType.BUY
                    and position_side.lower() == "buy"
                )
                or (
                    tracked_order.trade_type is TradeType.SELL
                    and position_side.lower() == "sell"
                )
            )
            else PositionAction.CLOSE
        )
        flat_fees = (
            []
            if fee_amount == Decimal("0")
            else [TokenAmount(amount=fee_amount, token=fee_asset)]
        )
        fee = TradeFeeBase.new_perpetual_fee(
            fee_schema=self.trade_fee_schema(),
            position_action=position_action,
            percent_token=fee_asset,
            flat_fees=flat_fees,
        )
        exec_price = trade_msg["price"] * price_factor
        exec_base_amount = trade_msg["size"] * amount_factor
        exec_quote_amount = exec_price * exec_base_amount
        exec_time = float(trade_msg["timestamp_ms"])
        exchange_order_id = (
            trade_msg["maker_order_sid"] if is_maker else trade_msg["taker_order_sid"]
        )
        trade_update: TradeUpdate = TradeUpdate(
            trade_id=trade_id,
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=exchange_order_id,
            trading_pair=tracked_order.trading_pair,
            fill_timestamp=exec_time,
            fill_price=exec_price,
            fill_base_amount=exec_base_amount,
            fill_quote_amount=exec_quote_amount,
            fee=fee,
        )
        return trade_update

    async def _process_position_message(self, position_msg: Dict[str, Any]):
        """
        Updates position
        """
        market_address = position_msg["market_addr"]
        trading_pair = await self.trading_pair_associated_to_market_address(
            market_address
        )
        if not trading_pair:
            raise IOError(f"No trading pair associated to market: {market_address}")
        trading_rule = self.trading_rules[trading_pair]
        price_factor, amount_factor = get_scale_factors(trading_rule, inverse=True)
        position_side = (
            PositionSide.LONG
            if position_msg["side"].lower() == "buy"
            else PositionSide.SHORT
        )
        amount = position_msg["size"] * amount_factor
        entry_price = position_msg["entry_price"] * price_factor
        leverage = position_msg.get("leverage", 1)
        unrealized_pnl = Decimal(position_msg["unrealized_pnl"]) * price_factor
        pos_key = self._perpetual_trading.position_key(trading_pair, position_side)
        if amount != s_decimal_0:
            position = Position(
                trading_pair=trading_pair,
                position_side=position_side,
                unrealized_pnl=unrealized_pnl,
                entry_price=entry_price,
                amount=amount,
                leverage=Decimal(leverage if leverage else 1),
            )
            self._perpetual_trading.set_position(pos_key, position)
        else:
            self._perpetual_trading.remove_position(pos_key)
        # Ekiden WS does not push balances
        safe_ensure_future(self._update_balances())

    async def _format_trading_rules(
        self, exchange_info_list: List[Dict[str, Any]]
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
                min_order_size = Decimal(market.get("min_order_size", 1)) / (
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

    def _initialize_trading_pair_symbols_from_exchange_info(
        self, exchange_info: List[Dict[str, Any]]
    ):
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
        if not self._initialized_rules:
            await self._update_trading_rules()
        market_addr = await self.market_address_associated_to_pair(trading_pair)
        trading_rule = self.trading_rules[trading_pair]
        price_factor, _ = get_scale_factors(trading_rule, inverse=True)
        path_url = CONSTANTS.MARKET_STATS.format(market_addr=market_addr)
        response = await self._api_get(path_url=path_url, limit_id=CONSTANTS.MARKET_STATS)
        price = response.get("current_price") or 0
        return float(price * price_factor)

    async def _trading_pair_position_mode_set(
        self, mode: PositionMode, trading_pair: str
    ) -> Tuple[bool, str]:
        if mode != PositionMode.ONEWAY:
            self.logger().warning(
                f"ekiden only supports the ONEWAY position modesupplied mode: {mode}",
            )
            return False, f"Invalid position mode: {mode}"
        else:
            self.logger().debug(
                f"ekiden switching position mode to "
                f"{mode} for {trading_pair} succeeded."
            )
            return True, "Success"

    async def _set_trading_pair_leverage(
        self, trading_pair: str, leverage: int
    ) -> Tuple[bool, str]:
        await self._update_trading_rules()
        market_addr = await self.market_address_associated_to_pair(trading_pair)
        nonce = self._nonce_provider.get_tracking_nonce()
        payload = {
            "type": IntentType.LEVERAGE_ASSIGN.value,
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
            output = result.get("output", {})
            if not output:
                return False, "No output data in response"
            sid = output.get("sid", "")
            if sid:
                return True, "Leverage assigned successfully"
            return False, "Unknown response from leverage assignment"
        except Exception as e:
            return False, f"Error assigning leverage for {trading_pair}: {e}"

    async def _fetch_last_fee_payment(
        self, trading_pair: str
    ) -> Tuple[int, Decimal, Decimal]:
        market_addr = await self.market_address_associated_to_pair(trading_pair)
        if not market_addr:
            return 0, Decimal("-1"), Decimal("-1")
        positions: List[Dict[str, Any]] = await self._api_get(
            path_url=CONSTANTS.USER_POSITIONS,
            params={"market_addr": market_addr},
            is_auth_required=True,
        )
        if not positions:
            return 0, Decimal("-1"), Decimal("-1")
        position = positions[0]
        current_funding_index = Decimal(position.get("funding_index", 0))
        entry_funding_index = Decimal(
            position.get("entry_funding_index", current_funding_index)
        )
        size = Decimal(position.get("size", 0))
        side = position.get("side", OrderSide.BUY.value)
        funding_index_diff = current_funding_index - entry_funding_index
        multiplier = Decimal(1) if side == OrderSide.BUY.value else Decimal(-1)
        payment = size * funding_index_diff * multiplier
        funding_rate = funding_index_diff * multiplier
        timestamp = int(position.get("timestamp_ms", 0))
        if payment == Decimal("0"):
            return 0, Decimal("-1"), Decimal("-1")
        return timestamp, funding_rate, payment

    @staticmethod
    def is_order_not_active(e: Exception) -> bool:
        if CONSTANTS.ORDER_NOT_ACTIVE_MSG in str(e):
            return True
        return False
