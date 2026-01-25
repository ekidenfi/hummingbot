import asyncio
import time
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from bidict import bidict

import hummingbot.connector.derivative.ekiden_perpetual.ekiden_perpetual_constants as CONSTANTS
from hummingbot.connector.derivative.ekiden_perpetual import ekiden_perpetual_web_utils as web_utils
from hummingbot.connector.derivative.ekiden_perpetual.ekiden_perpetual_api_order_book_data_source import (
    EkidenPerpetualAPIOrderBookDataSource,
)
from hummingbot.connector.derivative.ekiden_perpetual.ekiden_perpetual_auth import EkidenPerpetualAuth
from hummingbot.connector.derivative.ekiden_perpetual.ekiden_perpetual_constants import (
    MarginMode,
    OrderSide,
    OrderTypeString,
    TimeInForce,
)
from hummingbot.connector.derivative.ekiden_perpetual.ekiden_perpetual_user_stream_data_source import (
    EkidenPerpetualUserStreamDataSource,
)
from hummingbot.connector.derivative.position import Position
from hummingbot.connector.perpetual_derivative_py_base import PerpetualDerivativePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair, split_hb_trading_pair
from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode, PositionSide, PriceType, TradeType
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
        self._sub_account_address: Optional[str] = None
        self._last_empty_orderbook_warning: Dict[str, float] = {}
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
        await self._api_get(
            path_url=CONSTANTS.HEALTH_URL,
        )

    @property
    def check_network_request_path(self) -> str:
        return CONSTANTS.HEALTH_URL

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
        exchange_info: Dict[str, Any] = await self._make_trading_rules_request()
        trading_rules_list = await self._format_trading_rules(exchange_info)
        self._trading_rules.clear()
        for trading_rule in trading_rules_list:
            self._trading_rules[trading_rule.trading_pair] = trading_rule
        self._initialize_trading_pair_symbols_from_exchange_info(
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
        if not await self._ensure_sub_account_address():
            raise ValueError("Vault address not available, cannot cancel order")
        exchange_symbol = await self.exchange_symbol_associated_to_pair(
            tracked_order.trading_pair
        )
        cancel_request = {
            "symbol": exchange_symbol,
            "order_id": tracked_order.exchange_order_id,
            "order_link_id": tracked_order.client_order_id,
            "sub_account_address": self._sub_account_address,
        }
        cancel_response = await self._api_post(
            path_url=CONSTANTS.ORDER_CANCEL,
            data=cancel_request,
            is_auth_required=True,
        )
        return cancel_response.get("order_id") is not None

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
        if not await self._ensure_sub_account_address():
            raise ValueError("Vault address not available, cannot place order")

        exchange_symbol = await self.exchange_symbol_associated_to_pair(trading_pair)

        order_side = (
            OrderSide.BUY.value if trade_type is TradeType.BUY else OrderSide.SELL.value
        )
        order_type_str = (
            OrderTypeString.MARKET.value
            if order_type is OrderType.MARKET
            else OrderTypeString.LIMIT.value
        )

        place_request = {
            "symbol": exchange_symbol,
            "side": order_side,
            "order_type": order_type_str,
            "qty": str(amount),
            "price": str(price),
            "margin_mode": MarginMode.CROSS.value,
            "time_in_force": TimeInForce.GTC.value,
            "post_only": False,
            "reduce_only": position_action is PositionAction.CLOSE,
            "close_on_trigger": False,
            "order_link_id": order_id,
            "sub_account_address": self._sub_account_address,
            "expire_time": None,
        }

        if order_type is OrderType.MARKET:
            place_request["time_in_force"] = TimeInForce.IOC.value

        place_response = await self._api_post(
            path_url=CONSTANTS.ORDER_PLACE,
            data=place_request,
            is_auth_required=True,
        )

        exchange_order_id = place_response.get("order_id")
        if not exchange_order_id:
            raise ValueError(f"Failed to place order: {place_response}")

        return exchange_order_id, self.current_timestamp

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
                if not await self._ensure_sub_account_address():
                    return
                all_fills_response: List[Dict[str, Any]] = await self._api_get(
                    path_url=CONSTANTS.EXECUTION_LIST,
                    params={"sub_account_address": self._sub_account_address},
                    is_auth_required=True,
                )
                fills_list = all_fills_response.get("list", [])
                for trade_fill in fills_list:
                    self._process_trade_message(trade_fill)
            except asyncio.CancelledError:
                raise
            except Exception as request_error:
                self.logger().warning(
                    f"Failed to fetch trade updates. Error: {request_error}",
                    exc_info=request_error,
                )

    async def _update_order_status(self):
        open_orders = list(self._order_tracker.all_updatable_orders.values())
        if len(open_orders) == 0:
            return

        if not await self._ensure_sub_account_address():
            return

        try:
            order_list_response = await self._api_get(
                path_url=CONSTANTS.ORDER_REALTIME,
                params={"sub_account_address": self._sub_account_address},
                is_auth_required=True,
            )
            orders = order_list_response.get("list", [])
            for order_data in orders:
                self._process_order_update(order_data)
        except asyncio.CancelledError:
            raise
        except Exception as request_error:
            self.logger().warning(
                f"Failed to fetch order updates. Error: {request_error}",
                exc_info=request_error,
            )

    def _process_order_update(self, order_data: Dict[str, Any]):
        raw_status = order_data.get("order_status")
        order_status = CONSTANTS.ORDER_STATUSES.get(raw_status)
        if order_status is None:
            self.logger().warning(f"Unknown order status: {raw_status}")
            return
        exch_order_id = order_data.get("order_id")
        client_order_id = order_data.get("order_link_id")
        updatable_order = self._order_tracker.all_updatable_orders.get(client_order_id)

        if updatable_order is not None:
            new_order_update: OrderUpdate = OrderUpdate(
                trading_pair=updatable_order.trading_pair,
                update_timestamp=self.current_timestamp,
                new_state=order_status,
                client_order_id=updatable_order.client_order_id,
                exchange_order_id=exch_order_id,
            )
            self._order_tracker.process_order_update(new_order_update)

    async def _ensure_sub_account_address(self) -> bool:
        if self._sub_account_address is None:
            await self._update_balances()
            if self._sub_account_address is None:
                return False
        return True

    async def _update_balances(self):
        try:
            balance_response = await self._api_get(
                path_url=CONSTANTS.ACCOUNT_BALANCE,
                is_auth_required=True,
            )
            balance_list = balance_response.get("list", [])
            balance_data = next(
                (b for b in balance_list if b.get("account_type") == "cross")
            )

            available_balance = Decimal(balance_data.get("available_balance", "0"))
            vault_balance = Decimal(balance_data.get("vault_balance", "0"))
            self._sub_account_address = balance_data.get("sub_account_address")

            self._account_available_balances.clear()
            self._account_balances.clear()

            self._account_balances[CONSTANTS.CURRENCY] = vault_balance
            self._account_available_balances[CONSTANTS.CURRENCY] = available_balance
        except asyncio.CancelledError:
            raise
        except StopIteration:
            self.logger().warning("No cross balance found")
            return
        except Exception as request_error:
            self.logger().warning(
                f"Failed to fetch balance updates. Error: {request_error}",
                exc_info=request_error,
            )

    async def _update_positions(self):
        try:
            if not await self._ensure_sub_account_address():
                self.logger().warning(
                    "Vault address not available, cannot fetch positions"
                )
                return
            position_response = await self._api_get(
                path_url=CONSTANTS.POSITION_LIST,
                params={"sub_account_address": self._sub_account_address},
                is_auth_required=True,
            )
            positions = position_response.get("list", [])
            await self._parse_positions(positions)
        except asyncio.CancelledError:
            raise
        except Exception as request_error:
            self.logger().warning(
                f"Failed to fetch position updates. Error: {request_error}",
                exc_info=request_error,
            )

    async def _parse_positions(self, positions: List[Dict[str, Any]]):
        for pos in positions:
            symbol = pos["symbol"]
            trading_pair = await self.trading_pair_associated_to_exchange_symbol(symbol)
            side = pos["side"]
            position_side = (
                PositionSide.LONG if side.lower() == "Buy" else PositionSide.SHORT
            )
            amount = Decimal(pos["size"])
            entry_price = Decimal(pos["avg_price"])
            unrealized_pnl = Decimal(pos["unrealized_pnl"])
            leverage = Decimal(pos["leverage"])
            pos_key = self._perpetual_trading.position_key(trading_pair, position_side)
            if amount > 0:
                _position = Position(
                    trading_pair=trading_pair,
                    position_side=position_side,
                    unrealized_pnl=unrealized_pnl,
                    entry_price=entry_price,
                    amount=amount,
                    leverage=leverage,
                )
                self._perpetual_trading.set_position(pos_key, _position)
            else:
                self._perpetual_trading.remove_position(pos_key)

    async def _all_trade_updates_for_order(
        self, order: InFlightOrder
    ) -> List[TradeUpdate]:
        trade_updates = []

        if order.exchange_order_id is not None:
            try:
                all_fills_response = await self._request_order_fills(order=order)
                fills_list = all_fills_response.get("list", [])

                for fill_data in fills_list:
                    trade_update = self._parse_trade_update(
                        trade_msg=fill_data, tracked_order=order
                    )
                    trade_updates.append(trade_update)
            except IOError as ex:
                if not self._is_request_exception_related_to_time_synchronizer(
                    request_exception=ex
                ):
                    raise

        return trade_updates

    async def _request_order_fills(self, order: InFlightOrder) -> Dict[str, Any]:
        if not await self._ensure_sub_account_address():
            raise ValueError("Vault address not available, cannot request order fills")

        exchange_symbol = await self.exchange_symbol_associated_to_pair(
            order.trading_pair
        )
        params = {
            "sub_account_address": self._sub_account_address,
            "symbol": exchange_symbol,
            "order_id": order.exchange_order_id,
        }

        if order.client_order_id:
            params["order_link_id"] = order.client_order_id

        order_fills_response = await self._api_get(
            path_url=CONSTANTS.EXECUTION_LIST,
            params=params,
            is_auth_required=True,
        )
        return order_fills_response

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        exch_order = await self._request_order_status_data(tracked_order)
        if exch_order:
            raw_status = exch_order.get("order_status") or exch_order.get("status", "")
            exch_status = raw_status.lower()
            order_state = CONSTANTS.ORDER_STATUSES.get(exch_status)
            update_time_str = exch_order.get("updated_time") or exch_order.get(
                "created_time", "0"
            )
            update_timestamp = (
                float(update_time_str) / 1e3
                if update_time_str
                else self.current_timestamp
            )
            exch_order_id = exch_order.get("order_id") or exch_order.get("sid")
            return OrderUpdate(
                trading_pair=tracked_order.trading_pair,
                update_timestamp=update_timestamp,
                new_state=order_state if order_state else tracked_order.current_state,
                client_order_id=tracked_order.client_order_id,
                exchange_order_id=exch_order_id,
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
        exchange_symbol = await self.exchange_symbol_associated_to_pair(
            tracked_order.trading_pair
        )
        params = {
            "symbol": exchange_symbol,
            "order_id": tracked_order.exchange_order_id,
            "order_link_id": tracked_order.client_order_id,
            "sub_account_address": self._sub_account_address,
        }
        order_list_response = await self._api_get(
            path_url=CONSTANTS.ORDER_REALTIME,
            params=params,
            is_auth_required=True,
        )
        orders = order_list_response.get("list", [])
        if len(orders) > 0:
            return orders[0]
        return None

    async def _user_stream_event_listener(self):
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
                        for trade_msg in results:
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
        raw_status = order_msg["order_status"]
        order_status = CONSTANTS.ORDER_STATUSES.get(raw_status)
        if order_status is None:
            self.logger().warning(f"Unknown order status: {raw_status}")
            return
        exch_order_id = order_msg["order_id"]
        client_order_id = order_msg["order_link_id"]
        updatable_order = self._order_tracker.all_updatable_orders.get(client_order_id)
        if not updatable_order:
            return
        new_order_update: OrderUpdate = OrderUpdate(
            trading_pair=updatable_order.trading_pair,
            update_timestamp=self.current_timestamp,
            new_state=order_status,
            client_order_id=updatable_order.client_order_id,
            exchange_order_id=exch_order_id,
        )
        self._order_tracker.process_order_update(new_order_update)

    def _process_trade_message(self, trade_msg: Dict[str, Any]) -> None:
        client_order_id = trade_msg["order_link_id"]
        fillable_order = self._order_tracker.all_fillable_orders.get(client_order_id)
        if fillable_order is None:
            return
        trade_update = self._parse_trade_update(
            trade_msg=trade_msg, tracked_order=fillable_order
        )
        self._order_tracker.process_trade_update(trade_update)

    def _parse_trade_update(
        self, trade_msg: Dict, tracked_order: InFlightOrder
    ) -> TradeUpdate:
        trade_id = trade_msg["exec_id"]
        is_maker = trade_msg["is_maker"]
        fee_asset = tracked_order.quote_asset
        fee_amount = Decimal(trade_msg["fee_rate"])
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
        exec_price = Decimal(trade_msg["exec_price"])
        exec_base_amount = Decimal(trade_msg["exec_qty"])
        exec_quote_amount = exec_price * exec_base_amount
        exec_time = float(trade_msg["exec_time"])
        exchange_order_id = trade_msg["order_id"]
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
            is_taker=not is_maker,
        )
        return trade_update

    async def _process_position_message(self, position_msg: Dict[str, Any]):
        trading_pair = position_msg["symbol"]
        position_side = (
            PositionSide.LONG
            if position_msg["side"].lower() == "buy"
            else PositionSide.SHORT
        )
        amount = Decimal(position_msg["size"])
        entry_price = Decimal(position_msg["avg_price"])
        leverage = position_msg["leverage"]
        unrealized_pnl = Decimal(position_msg["unrealized_pnl"])
        pos_key = self._perpetual_trading.position_key(trading_pair, position_side)
        if amount != s_decimal_0:
            position = Position(
                trading_pair=trading_pair,
                position_side=position_side,
                unrealized_pnl=unrealized_pnl,
                entry_price=entry_price,
                amount=amount,
                leverage=Decimal(leverage),
            )
            self._perpetual_trading.set_position(pos_key, position)
        else:
            self._perpetual_trading.remove_position(pos_key)
        # Ekiden WS does not push balances
        safe_ensure_future(self._update_balances())

    async def _format_trading_rules(
        self, exchange_info: Dict[str, Any]
    ) -> List[TradingRule]:
        trading_rules: List[TradingRule] = []
        if not exchange_info.get("list"):
            raise ValueError("Exchange info is not valid")
        else:
            exchange_info = exchange_info.get("list")

        try:
            for market in exchange_info:
                symbol = market.get("symbol")
                if not symbol:
                    self.logger().warning(
                        f"Skipping market with missing symbol: {market}"
                    )
                    continue
                trading_pair = await self.trading_pair_associated_to_exchange_symbol(
                    symbol=symbol
                )
                base_decimals = abs(
                    Decimal(market.get("mark_price", 0)).as_tuple().exponent
                )
                quote_decimals = CONSTANTS.QUOTE_DECIMALS
                min_order_size = Decimal(10) ** (-base_decimals)
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
        self, exchange_info: Dict[str, Any]
    ):
        if not exchange_info.get("list"):
            raise ValueError("Exchange info is not valid")
        else:
            exchange_info = exchange_info.get("list")

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
        exchange_symbol = await self.exchange_symbol_associated_to_pair(trading_pair)
        response = await self._api_get(
            path_url=CONSTANTS.MARKET_STATS, params={"symbol": exchange_symbol}
        )
        ticker_list = response.get("list", [])
        data = ticker_list[0]
        price = data["last_price"]
        return float(price)

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
        if not await self._ensure_sub_account_address():
            return False, "Vault address not available"
        exchange_symbol = await self.exchange_symbol_associated_to_pair(trading_pair)
        set_leverage_request = {
            "symbol": exchange_symbol,
            "leverage": str(leverage),
            "sub_account_address": self._sub_account_address,
        }
        try:
            set_leverage_response = await self._api_post(
                path_url=CONSTANTS.POSITION_SET_LEVERAGE,
                data=set_leverage_request,
                is_auth_required=True,
            )
            success = set_leverage_response.get("success", False)
            if success:
                return True, "Success"
            else:
                return False, "Failed to set leverage"
        except Exception as e:
            self.logger().error(
                f"Error setting leverage for {trading_pair}: {e}",
                exc_info=True,
            )
            return False, str(e)

    async def _fetch_last_fee_payment(
        self, trading_pair: str
    ) -> Tuple[int, Decimal, Decimal]:
        if not await self._ensure_sub_account_address():
            return 0, s_decimal_0, s_decimal_0
        exchange_symbol = await self.exchange_symbol_associated_to_pair(trading_pair)
        position_response = await self._api_get(
            path_url=CONSTANTS.POSITION_LIST,
            params={
                "symbol": exchange_symbol,
                "sub_account_address": self._sub_account_address,
            },
            is_auth_required=True,
        )
        positions = position_response.get("list", [])
        if len(positions) == 0:
            return 0, s_decimal_0, s_decimal_0

        position = positions[0]
        updated_time_str = position.get("updated_time", "0")
        updated_time = int(float(updated_time_str)) if updated_time_str else 0

        unrealized_funding_str = position.get("unrealized_funding", "0")
        unrealized_funding = Decimal(unrealized_funding_str)

        realized_pnl_cum_str = position.get("realized_pnl_cum", "0")
        realized_pnl_cum = Decimal(realized_pnl_cum_str)

        return updated_time, unrealized_funding, realized_pnl_cum

    def get_price(self, trading_pair: str, is_buy: bool) -> Decimal:
        """
        Override get_price to use mark price when orderbook is empty.
        """
        try:
            order_book = self.get_order_book(trading_pair)
            if is_buy:
                ask_entries = list(order_book.ask_entries())
                if not ask_entries:
                    return self._get_price_from_mark_price(trading_pair, is_buy)
            else:
                bid_entries = list(order_book.bid_entries())
                if not bid_entries:
                    return self._get_price_from_mark_price(trading_pair, is_buy)

            return super().get_price(trading_pair, is_buy)
        except (ValueError, KeyError):
            return self._get_price_from_mark_price(trading_pair, is_buy)
        except Exception:
            return super().get_price(trading_pair, is_buy)

    def get_price_by_type(self, trading_pair: str, price_type: PriceType) -> Decimal:
        """
        Override get_price_by_type to use mark price when orderbook is empty.
        """
        if price_type is PriceType.BestBid:
            return self.get_price(trading_pair, False)
        elif price_type is PriceType.BestAsk:
            return self.get_price(trading_pair, True)
        elif price_type is PriceType.MidPrice:
            bid_price = self.get_price(trading_pair, False)
            ask_price = self.get_price(trading_pair, True)
            return (bid_price + ask_price) / Decimal("2")
        elif price_type is PriceType.LastTrade:
            try:
                order_book = self.get_order_book(trading_pair)
                return Decimal(order_book.last_trade_price)
            except (ValueError, KeyError):
                try:
                    funding_info = self.get_funding_info(trading_pair)
                    if funding_info and funding_info.mark_price:
                        return Decimal(str(funding_info.mark_price))
                except Exception:
                    pass
                return super().get_price_by_type(trading_pair, price_type)
        else:
            return super().get_price_by_type(trading_pair, price_type)

    def _get_price_from_mark_price(self, trading_pair: str, is_buy: bool) -> Decimal:
        current_time = time.time()
        warning_key = f"{trading_pair}_{is_buy}"
        last_warning_time = self._last_empty_orderbook_warning.get(warning_key, 0)

        if current_time - last_warning_time > 60:
            self.logger().debug(
                f"{'Ask' if is_buy else 'Bid'} orderbook for {trading_pair} is empty. "
                f"Using mark price for order placement."
            )
            self._last_empty_orderbook_warning[warning_key] = current_time

        try:
            funding_info = self.get_funding_info(trading_pair)
            if funding_info and funding_info.mark_price:
                mark_price = Decimal(str(funding_info.mark_price))
                return self.quantize_order_price(trading_pair, mark_price)
        except Exception as e:
            self.logger().debug(
                f"Could not get mark price for {trading_pair}: {e}. "
                f"Falling back to base implementation."
            )

        return super().get_price(trading_pair, is_buy)
