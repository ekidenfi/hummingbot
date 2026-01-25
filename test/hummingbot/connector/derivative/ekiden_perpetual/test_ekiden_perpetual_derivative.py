import json
import re
from decimal import Decimal
from typing import Callable, List, Optional, Tuple
from unittest.mock import MagicMock, patch

from aioresponses import aioresponses
from aioresponses.core import RequestCall
from bidict import bidict

import hummingbot.connector.derivative.ekiden_perpetual.ekiden_perpetual_constants as CONSTANTS
import hummingbot.connector.derivative.ekiden_perpetual.ekiden_perpetual_web_utils as web_utils
from hummingbot.connector.derivative.ekiden_perpetual.ekiden_perpetual_api_order_book_data_source import (
    EkidenPerpetualAPIOrderBookDataSource,
)
from hummingbot.connector.derivative.ekiden_perpetual.ekiden_perpetual_derivative import EkidenPerpetualDerivative
from hummingbot.connector.derivative.position import Position
from hummingbot.connector.test_support.perpetual_derivative_test import AbstractPerpetualDerivativeTests
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, PositionMode, PositionSide, PriceType, TradeType
from hummingbot.core.data_type.funding_info import FundingInfo
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount, TradeFeeBase


class EkidenPerpetualDerivativeTests(
    AbstractPerpetualDerivativeTests.PerpetualDerivativeTests
):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.base_asset = "ETH"
        cls.quote_asset = "USDC"
        cls.trading_pair = combine_to_hb_trading_pair(cls.base_asset, cls.quote_asset)
        cls.exchange_trading_pair = f"{cls.base_asset}-{cls.quote_asset}"
        cls.sub_account_address = "0x1234567890abcdef1234567890abcdef12345678"

    @property
    def expected_supported_position_modes(self) -> List[PositionMode]:
        return [PositionMode.ONEWAY]

    @property
    def expected_latest_price(self):
        return 3000.0

    @property
    def expected_exchange_order_id(self):
        return "test_exchange_order_id"

    @property
    def expected_fill_fee(self) -> TradeFeeBase:
        return AddedToCostTradeFee(
            percent_token=self.quote_asset,
            flat_fees=[TokenAmount(token=self.quote_asset, amount=Decimal("0.001"))],
        )

    @property
    def expected_fill_trade_id(self) -> str:
        return "exec_123"

    @property
    def is_order_fill_http_update_included_in_status_update(self) -> bool:
        return False

    @property
    def is_order_fill_http_update_executed_during_websocket_order_event_processing(
        self,
    ) -> bool:
        return False

    @property
    def all_symbols_url(self):
        url = web_utils.public_rest_url(path_url=CONSTANTS.MARKET_INFO)
        return url

    @property
    def latest_prices_url(self):
        url = web_utils.public_rest_url(path_url=CONSTANTS.MARKET_STATS)
        url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + ".*")
        return url

    @property
    def network_status_url(self):
        url = web_utils.public_rest_url(path_url=CONSTANTS.HEALTH_URL)
        return url

    @property
    def trading_rules_url(self):
        url = web_utils.public_rest_url(path_url=CONSTANTS.MARKET_INFO)
        return url

    @property
    def order_creation_url(self):
        url = web_utils.private_rest_url(path_url=CONSTANTS.ORDER_PLACE)
        url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + ".*")
        return url

    @property
    def balance_url(self):
        url = web_utils.private_rest_url(path_url=CONSTANTS.ACCOUNT_BALANCE)
        return url

    @property
    def funding_info_url(self):
        url = web_utils.public_rest_url(path_url=CONSTANTS.MARKET_STATS)
        url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + ".*")
        return url

    @property
    def funding_payment_url(self):
        url = web_utils.private_rest_url(path_url=CONSTANTS.POSITION_LIST)
        url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + ".*")
        return url

    def setUp(self) -> None:
        super().setUp()
        self.exchange = EkidenPerpetualDerivative(
            aptos_private_key="test_private_key",
            trading_pairs=[self.trading_pair],
            trading_required=True,
        )

        EkidenPerpetualAPIOrderBookDataSource._trading_pair_symbol_map = {
            CONSTANTS.DOMAIN: bidict({self.exchange_trading_pair: self.trading_pair})
        }

        self.exchange._set_current_timestamp(1640780000)
        self.exchange.logger().setLevel(1)
        self.exchange.logger().addHandler(self)
        self.exchange._order_tracker.logger().setLevel(1)
        self.exchange._order_tracker.logger().addHandler(self)

    def tearDown(self) -> None:
        EkidenPerpetualAPIOrderBookDataSource._trading_pair_symbol_map = {}
        super().tearDown()

    @property
    def all_symbols_request_mock_response(self):
        return {
            "list": [
                {
                    "symbol": self.exchange_trading_pair,
                    "mark_price": "3000.0",
                    "index_price": "2999.5",
                    "funding_rate": "0.0001",
                    "next_funding_time": self.target_funding_info_next_funding_utc_timestamp,
                }
            ]
        }

    @property
    def latest_prices_request_mock_response(self):
        return {
            "list": [
                {
                    "symbol": self.exchange_trading_pair,
                    "last_price": str(self.expected_latest_price),
                    "mark_price": "3000.0",
                    "index_price": "2999.5",
                    "funding_rate": "0.0001",
                    "next_funding_time": self.target_funding_info_next_funding_utc_timestamp,
                }
            ]
        }

    @property
    def network_status_request_successful_mock_response(self):
        return {"status": "ok"}

    @property
    def trading_rules_request_mock_response(self):
        return {
            "list": [
                {
                    "symbol": self.exchange_trading_pair,
                    "mark_price": "3000.0",
                    "index_price": "2999.5",
                    "funding_rate": "0.0001",
                    "next_funding_time": self.target_funding_info_next_funding_utc_timestamp,
                }
            ]
        }

    @property
    def order_creation_request_successful_mock_response(self):
        return {
            "order_id": self.expected_exchange_order_id,
            "order_link_id": "test_order_id",
        }

    @property
    def balance_request_mock_response_for_base_and_quote(self):
        return {
            "list": [
                {
                    "account_type": "cross",
                    "available_balance": "1000.0",
                    "vault_balance": "2000.0",
                    "sub_account_address": self.sub_account_address,
                }
            ]
        }

    @property
    def funding_info_mock_response(self):
        return {
            "list": [
                {
                    "symbol": self.exchange_trading_pair,
                    "index_price": str(self.target_funding_info_index_price),
                    "mark_price": str(self.target_funding_info_mark_price),
                    "next_funding_time": self.target_funding_info_next_funding_utc_timestamp,
                    "funding_rate": str(self.target_funding_info_rate),
                }
            ]
        }

    @property
    def empty_funding_payment_mock_response(self):
        return {"list": []}

    @property
    def funding_payment_mock_response(self):
        return {
            "list": [
                {
                    "symbol": self.exchange_trading_pair,
                    "updated_time": str(self.target_funding_payment_timestamp),
                    "unrealized_funding": str(self.target_funding_payment_funding_rate),
                    "realized_pnl_cum": str(self.target_funding_payment_payment_amount),
                }
            ]
        }

    def position_event_for_full_fill_websocket_update(
        self, order: InFlightOrder, unrealized_pnl: float
    ):
        return {
            "symbol": self.exchange_trading_pair,
            "side": "Buy" if order.trade_type == TradeType.BUY else "Sell",
            "size": str(order.amount),
            "avg_price": str(order.price),
            "unrealized_pnl": str(unrealized_pnl),
            "leverage": "1",
        }

    def order_event_for_full_fill_websocket_update(self, order: InFlightOrder):
        return {
            "topic": "order",
            "data": [
                {
                    "order_id": order.exchange_order_id or "test_order_id",
                    "order_link_id": order.client_order_id,
                    "order_status": "Filled",
                    "symbol": self.exchange_trading_pair,
                    "side": "Buy" if order.trade_type == TradeType.BUY else "Sell",
                    "order_type": "Limit",
                    "qty": str(order.amount),
                    "price": str(order.price),
                }
            ],
        }

    def trade_event_for_full_fill_websocket_update(self, order: InFlightOrder):
        return {
            "topic": "execution",
            "data": [
                {
                    "exec_id": "exec_123",
                    "order_id": order.exchange_order_id or "test_order_id",
                    "order_link_id": order.client_order_id,
                    "exec_price": str(order.price),
                    "exec_qty": str(order.amount),
                    "exec_time": "1640780000.0",
                    "fee_rate": "0.001",
                    "is_maker": False,
                    "side": "Buy" if order.trade_type == TradeType.BUY else "Sell",
                }
            ],
        }

    def configure_completely_filled_order_status_response(
        self,
        order: InFlightOrder,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> str:
        url = web_utils.private_rest_url(path_url=CONSTANTS.ORDER_REALTIME)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + ".*")
        response = {
            "list": [
                {
                    "order_id": order.exchange_order_id,
                    "order_link_id": order.client_order_id,
                    "order_status": "Filled",
                    "symbol": self.exchange_trading_pair,
                    "side": "Buy" if order.trade_type == TradeType.BUY else "Sell",
                    "order_type": "Limit",
                    "qty": str(order.amount),
                    "price": str(order.price),
                    "updated_time": "1640780000000",
                }
            ]
        }
        mock_api.get(regex_url, body=json.dumps(response), callback=callback)
        return url

    def configure_full_fill_trade_response(
        self,
        order: InFlightOrder,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> str:
        url = web_utils.private_rest_url(path_url=CONSTANTS.EXECUTION_LIST)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + ".*")
        response = {
            "list": [
                {
                    "exec_id": "exec_123",
                    "order_id": order.exchange_order_id,
                    "order_link_id": order.client_order_id,
                    "exec_price": str(order.price),
                    "exec_qty": str(order.amount),
                    "exec_time": "1640780000.0",
                    "fee_rate": "0.001",
                    "is_maker": False,
                    "side": "Buy" if order.trade_type == TradeType.BUY else "Sell",
                }
            ]
        }
        mock_api.get(regex_url, body=json.dumps(response), callback=callback)
        return url

    def configure_successful_set_position_mode(
        self,
        position_mode: PositionMode,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ):
        pass

    def configure_failed_set_position_mode(
        self,
        position_mode: PositionMode,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> Tuple[str, str]:
        return "", ""

    def configure_successful_set_leverage(
        self,
        leverage: int,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ):
        url = web_utils.private_rest_url(path_url=CONSTANTS.POSITION_SET_LEVERAGE)
        mock_api.post(
            re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + ".*"),
            body=json.dumps({"success": True}),
            callback=callback,
        )

    def configure_failed_set_leverage(
        self,
        leverage: int,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> Tuple[str, str]:
        url = web_utils.private_rest_url(path_url=CONSTANTS.POSITION_SET_LEVERAGE)
        mock_api.post(
            re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + ".*"),
            body=json.dumps({"success": False, "message": "Failed to set leverage"}),
            callback=callback,
        )
        return url, "Failed to set leverage"

    def funding_info_event_for_websocket_update(self):
        return {
            "topic": "ticker.ETH-USDC",
            "data": {
                "symbol": self.exchange_trading_pair,
                "index_price": str(self.target_funding_info_index_price_ws_updated),
                "mark_price": str(self.target_funding_info_mark_price_ws_updated),
                "next_funding_time": self.target_funding_info_next_funding_utc_timestamp_ws_updated,
                "funding_rate": str(self.target_funding_info_rate_ws_updated),
            },
        }

    def _get_market_info_mock_response(self):
        return {
            "list": [
                {
                    "symbol": self.exchange_trading_pair,
                    "mark_price": "3000.0",
                    "index_price": "2999.5",
                    "funding_rate": "0.0001",
                    "next_funding_time": self.target_funding_info_next_funding_utc_timestamp,
                }
            ]
        }

    def _get_balance_mock_response(self):
        return {
            "list": [
                {
                    "account_type": "cross",
                    "available_balance": "1000.0",
                    "vault_balance": "2000.0",
                    "sub_account_address": self.sub_account_address,
                }
            ]
        }

    def _get_position_mock_response(self):
        return {
            "list": [
                {
                    "symbol": self.exchange_trading_pair,
                    "side": "Buy",
                    "size": "1.0",
                    "avg_price": "3000.0",
                    "unrealized_pnl": "10.0",
                    "leverage": "1",
                }
            ]
        }

    def _get_order_mock_response(self):
        return {
            "list": [
                {
                    "order_id": self.expected_exchange_order_id,
                    "order_link_id": "test_order_id",
                    "order_status": "New",
                    "symbol": self.exchange_trading_pair,
                    "side": "Buy",
                    "order_type": "Limit",
                    "qty": "1.0",
                    "price": "3000.0",
                    "created_time": "1640780000000",
                    "updated_time": "1640780000000",
                }
            ]
        }

    def _get_trade_mock_response(self):
        return {
            "list": [
                {
                    "exec_id": "exec_123",
                    "order_id": self.expected_exchange_order_id,
                    "order_link_id": "test_order_id",
                    "exec_price": "3000.0",
                    "exec_qty": "1.0",
                    "exec_time": "1640780000.0",
                    "fee_rate": "0.001",
                    "is_maker": False,
                    "side": "Buy",
                }
            ]
        }

    def _get_funding_info_mock_response(self):
        return {
            "list": [
                {
                    "symbol": self.exchange_trading_pair,
                    "index_price": "2999.5",
                    "mark_price": "3000.0",
                    "next_funding_time": self.target_funding_info_next_funding_utc_timestamp,
                    "funding_rate": "0.0001",
                }
            ]
        }

    def _simulate_trading_rules_initialized(self):
        self.exchange._trading_rules = {
            self.trading_pair: TradingRule(
                trading_pair=self.trading_pair,
                min_order_size=Decimal("0.01"),
                min_base_amount_increment=Decimal("0.01"),
                min_price_increment=Decimal("0.01"),
                buy_order_collateral_token=self.quote_asset,
                sell_order_collateral_token=self.quote_asset,
            )
        }
        self.exchange._initialized_rules = True

    def _simulate_sub_account_address_set(self):
        self.exchange._sub_account_address = self.sub_account_address

    @aioresponses()
    def test_get_price_with_empty_orderbook_uses_mark_price(self, mock_api):
        self._simulate_trading_rules_initialized()
        self._simulate_sub_account_address_set()

        funding_info_url = web_utils.public_rest_url(
            path_url=CONSTANTS.MARKET_STATS,
            params={"symbol": self.exchange_trading_pair},
        )
        mock_api.get(
            re.compile(
                f"^{funding_info_url}".replace(".", r"\.").replace("?", r"\?") + ".*"
            ),
            body=json.dumps(self._get_funding_info_mock_response()),
        )

        order_book = MagicMock()
        order_book.ask_entries.return_value = iter([])
        order_book.bid_entries.return_value = iter([])
        self.exchange._order_book_tracker.order_books = {self.trading_pair: order_book}

        self.exchange._perpetual_trading.initialize_funding_info(
            FundingInfo(
                trading_pair=self.trading_pair,
                index_price=Decimal("2999.5"),
                mark_price=Decimal("3000.0"),
                next_funding_utc_timestamp=self.target_funding_info_next_funding_utc_timestamp,
                rate=Decimal("0.0001"),
            )
        )

        price = self.exchange.get_price(self.trading_pair, True)
        self.assertEqual(price, Decimal("3000.0"))

    @aioresponses()
    def test_get_price_with_empty_orderbook_uses_mark_price_sell(self, mock_api):
        self._simulate_trading_rules_initialized()
        self._simulate_sub_account_address_set()

        funding_info_url = web_utils.public_rest_url(
            path_url=CONSTANTS.MARKET_STATS,
            params={"symbol": self.exchange_trading_pair},
        )
        mock_api.get(
            re.compile(
                f"^{funding_info_url}".replace(".", r"\.").replace("?", r"\?") + ".*"
            ),
            body=json.dumps(self._get_funding_info_mock_response()),
        )

        order_book = MagicMock()
        order_book.ask_entries.return_value = iter([])
        order_book.bid_entries.return_value = iter([])
        self.exchange._order_book_tracker.order_books = {self.trading_pair: order_book}

        self.exchange._perpetual_trading.initialize_funding_info(
            FundingInfo(
                trading_pair=self.trading_pair,
                index_price=Decimal("2999.5"),
                mark_price=Decimal("3000.0"),
                next_funding_utc_timestamp=self.target_funding_info_next_funding_utc_timestamp,
                rate=Decimal("0.0001"),
            )
        )

        price = self.exchange.get_price(self.trading_pair, False)
        self.assertEqual(price, Decimal("3000.0"))

    @aioresponses()
    def test_get_price_by_type_mid_price_with_empty_orderbook(self, mock_api):
        self._simulate_trading_rules_initialized()
        self._simulate_sub_account_address_set()

        funding_info_url = web_utils.public_rest_url(
            path_url=CONSTANTS.MARKET_STATS,
            params={"symbol": self.exchange_trading_pair},
        )
        mock_api.get(
            re.compile(
                f"^{funding_info_url}".replace(".", r"\.").replace("?", r"\?") + ".*"
            ),
            body=json.dumps(self._get_funding_info_mock_response()),
        )

        order_book = MagicMock()
        order_book.ask_entries.return_value = iter([])
        order_book.bid_entries.return_value = iter([])
        self.exchange._order_book_tracker.order_books = {self.trading_pair: order_book}

        self.exchange._perpetual_trading.initialize_funding_info(
            FundingInfo(
                trading_pair=self.trading_pair,
                index_price=Decimal("2999.5"),
                mark_price=Decimal("3000.0"),
                next_funding_utc_timestamp=self.target_funding_info_next_funding_utc_timestamp,
                rate=Decimal("0.0001"),
            )
        )

        mid_price = self.exchange.get_price_by_type(
            self.trading_pair, PriceType.MidPrice
        )
        self.assertEqual(mid_price, Decimal("3000.0"))

    @aioresponses()
    def test_get_price_by_type_last_trade_with_empty_orderbook(self, mock_api):
        self._simulate_trading_rules_initialized()
        self._simulate_sub_account_address_set()

        funding_info_url = web_utils.public_rest_url(
            path_url=CONSTANTS.MARKET_STATS,
            params={"symbol": self.exchange_trading_pair},
        )
        mock_api.get(
            re.compile(
                f"^{funding_info_url}".replace(".", r"\.").replace("?", r"\?") + ".*"
            ),
            body=json.dumps(self._get_funding_info_mock_response()),
        )

        order_book = MagicMock()
        order_book.last_trade_price = 0.0
        self.exchange._order_book_tracker.order_books = {self.trading_pair: order_book}

        self.exchange._perpetual_trading.initialize_funding_info(
            FundingInfo(
                trading_pair=self.trading_pair,
                index_price=Decimal("2999.5"),
                mark_price=Decimal("3000.0"),
                next_funding_utc_timestamp=self.target_funding_info_next_funding_utc_timestamp,
                rate=Decimal("0.0001"),
            )
        )

        last_trade_price = self.exchange.get_price_by_type(
            self.trading_pair, PriceType.LastTrade
        )
        self.assertEqual(last_trade_price, Decimal("3000.0"))

    @aioresponses()
    def test_ensure_sub_account_address_success(self, mock_api):
        url = web_utils.private_rest_url(path_url=CONSTANTS.ACCOUNT_BALANCE)
        mock_api.get(
            url,
            body=json.dumps(self._get_balance_mock_response()),
        )

        result = self.async_run_with_timeout(
            self.exchange._ensure_sub_account_address()
        )
        self.assertTrue(result)
        self.assertEqual(self.exchange._sub_account_address, self.sub_account_address)

    @aioresponses()
    def test_ensure_sub_account_address_failure(self, mock_api):
        url = web_utils.private_rest_url(path_url=CONSTANTS.ACCOUNT_BALANCE)
        mock_api.get(
            url,
            body=json.dumps({"list": []}),
        )

        result = self.async_run_with_timeout(
            self.exchange._ensure_sub_account_address()
        )
        self.assertFalse(result)
        self.assertIsNone(self.exchange._sub_account_address)

    @aioresponses()
    def test_place_order_requires_sub_account_address(self, mock_api):
        self._simulate_trading_rules_initialized()
        self.exchange._sub_account_address = None

        with self.assertRaises(ValueError) as context:
            self.async_run_with_timeout(
                self.exchange._place_order(
                    order_id="test_order",
                    trading_pair=self.trading_pair,
                    amount=Decimal("1.0"),
                    trade_type=TradeType.BUY,
                    order_type=OrderType.LIMIT,
                    price=Decimal("3000.0"),
                )
            )

        self.assertIn("Vault address not available", str(context.exception))

    @aioresponses()
    def test_update_balances(self, mock_api):
        url = web_utils.private_rest_url(path_url=CONSTANTS.ACCOUNT_BALANCE)
        mock_api.get(
            url,
            body=json.dumps(self._get_balance_mock_response()),
        )

        self.async_run_with_timeout(self.exchange._update_balances())

        self.assertEqual(self.exchange._sub_account_address, self.sub_account_address)
        self.assertEqual(
            self.exchange._account_balances[CONSTANTS.CURRENCY],
            Decimal("2000.0"),
        )
        self.assertEqual(
            self.exchange._account_available_balances[CONSTANTS.CURRENCY],
            Decimal("1000.0"),
        )

    @aioresponses()
    def test_update_balances_without_sub_account(self, mock_api):
        url = web_utils.private_rest_url(path_url=CONSTANTS.ACCOUNT_BALANCE)
        mock_api.get(
            url,
            body=json.dumps({"list": []}),
        )

        self.async_run_with_timeout(self.exchange._update_balances())

        self.assertIsNone(self.exchange._sub_account_address)

    @aioresponses()
    def test_update_balances_stop_iteration(self, mock_api):
        url = web_utils.private_rest_url(path_url=CONSTANTS.ACCOUNT_BALANCE)
        mock_api.get(
            url,
            body=json.dumps({"list": [{"account_type": "isolated"}]}),
        )

        self.async_run_with_timeout(self.exchange._update_balances())

        self.assertIsNone(self.exchange._sub_account_address)
        self.assertTrue(self._is_logged("WARNING", "No cross balance found"))

    @aioresponses()
    def test_update_positions(self, mock_api):
        self._simulate_sub_account_address_set()
        url = web_utils.private_rest_url(path_url=CONSTANTS.POSITION_LIST)
        mock_api.get(
            re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + ".*"),
            body=json.dumps(self._get_position_mock_response()),
        )

        self.async_run_with_timeout(self.exchange._update_positions())

        position = self.exchange._perpetual_trading.get_position(
            self.trading_pair, PositionSide.LONG
        )
        self.assertIsNotNone(position)
        self.assertEqual(position.amount, Decimal("1.0"))
        self.assertEqual(position.entry_price, Decimal("3000.0"))

    @aioresponses()
    def test_parse_positions_long(self, mock_api):
        positions = [
            {
                "symbol": self.exchange_trading_pair,
                "side": "Buy",
                "size": "1.0",
                "avg_price": "3000.0",
                "unrealized_pnl": "10.0",
                "leverage": "2",
            }
        ]

        self.async_run_with_timeout(self.exchange._parse_positions(positions))

        position = self.exchange._perpetual_trading.get_position(
            self.trading_pair, PositionSide.LONG
        )
        self.assertIsNotNone(position)
        self.assertEqual(position.amount, Decimal("1.0"))
        self.assertEqual(position.entry_price, Decimal("3000.0"))
        self.assertEqual(position.unrealized_pnl, Decimal("10.0"))
        self.assertEqual(position.leverage, Decimal("2"))

    @aioresponses()
    def test_parse_positions_short(self, mock_api):
        positions = [
            {
                "symbol": self.exchange_trading_pair,
                "side": "Sell",
                "size": "1.0",
                "avg_price": "3000.0",
                "unrealized_pnl": "-10.0",
                "leverage": "3",
            }
        ]

        self.async_run_with_timeout(self.exchange._parse_positions(positions))

        position = self.exchange._perpetual_trading.get_position(
            self.trading_pair, PositionSide.SHORT
        )
        self.assertIsNotNone(position)
        self.assertEqual(position.amount, Decimal("1.0"))
        self.assertEqual(position.entry_price, Decimal("3000.0"))
        self.assertEqual(position.unrealized_pnl, Decimal("-10.0"))
        self.assertEqual(position.leverage, Decimal("3"))

    @aioresponses()
    def test_parse_positions_zero_amount(self, mock_api):
        self._simulate_sub_account_address_set()
        self.exchange._perpetual_trading.set_position(
            self.exchange._perpetual_trading.position_key(
                self.trading_pair, PositionSide.LONG
            ),
            Position(
                trading_pair=self.trading_pair,
                position_side=PositionSide.LONG,
                amount=Decimal("1.0"),
                entry_price=Decimal("3000.0"),
                unrealized_pnl=Decimal("10.0"),
                leverage=Decimal("1"),
            ),
        )

        positions = [
            {
                "symbol": self.exchange_trading_pair,
                "side": "Buy",
                "size": "0.0",
                "avg_price": "3000.0",
                "unrealized_pnl": "0.0",
                "leverage": "1",
            }
        ]

        self.async_run_with_timeout(self.exchange._parse_positions(positions))

        position = self.exchange._perpetual_trading.get_position(
            self.trading_pair, PositionSide.LONG
        )
        self.assertIsNone(position)

    @aioresponses()
    def test_process_order_update(self, mock_api):
        order = InFlightOrder(
            client_order_id="test_order",
            exchange_order_id="exch_order",
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("1.0"),
            price=Decimal("3000.0"),
            creation_timestamp=1640780000,
        )
        self.exchange._order_tracker.start_tracking_order(order)

        order_data = {
            "order_id": "exch_order",
            "order_link_id": "test_order",
            "order_status": "Filled",
        }

        self.exchange._process_order_update(order_data)

        self.assertEqual(order.current_state, OrderState.FILLED)

    @aioresponses()
    def test_process_trade_message(self, mock_api):
        order = InFlightOrder(
            client_order_id="test_order",
            exchange_order_id="exch_order",
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("1.0"),
            price=Decimal("3000.0"),
            creation_timestamp=1640780000,
        )
        self.exchange._order_tracker.start_tracking_order(order)

        trade_msg = {
            "exec_id": "exec_123",
            "order_id": "exch_order",
            "order_link_id": "test_order",
            "exec_price": "3000.0",
            "exec_qty": "1.0",
            "exec_time": "1640780000.0",
            "fee_rate": "0.001",
            "is_maker": False,
            "side": "Buy",
        }

        self.exchange._process_trade_message(trade_msg)

        self.assertEqual(len(self.order_filled_logger.event_log), 1)

    @aioresponses()
    def test_parse_trade_update(self, mock_api):
        order = InFlightOrder(
            client_order_id="test_order",
            exchange_order_id="exch_order",
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("1.0"),
            price=Decimal("3000.0"),
            creation_timestamp=1640780000,
        )

        trade_msg = {
            "exec_id": "exec_123",
            "order_id": "exch_order",
            "order_link_id": "test_order",
            "exec_price": "3000.0",
            "exec_qty": "1.0",
            "exec_time": "1640780000.0",
            "fee_rate": "0.001",
            "is_maker": False,
            "side": "Buy",
        }

        trade_update = self.exchange._parse_trade_update(trade_msg, order)

        self.assertEqual(trade_update.trade_id, "exec_123")
        self.assertEqual(trade_update.fill_price, Decimal("3000.0"))
        self.assertEqual(trade_update.fill_base_amount, Decimal("1.0"))
        self.assertEqual(trade_update.is_taker, True)

    @aioresponses()
    def test_request_order_fills(self, mock_api):
        self._simulate_sub_account_address_set()
        order = InFlightOrder(
            client_order_id="test_order",
            exchange_order_id="exch_order",
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("1.0"),
            price=Decimal("3000.0"),
            creation_timestamp=1640780000,
        )

        url = web_utils.private_rest_url(path_url=CONSTANTS.EXECUTION_LIST)
        mock_api.get(
            re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + ".*"),
            body=json.dumps(self._get_trade_mock_response()),
        )

        result = self.async_run_with_timeout(self.exchange._request_order_fills(order))

        self.assertIn("list", result)
        self.assertEqual(len(result["list"]), 1)

    @aioresponses()
    def test_fetch_last_fee_payment(self, mock_api):
        self._simulate_sub_account_address_set()
        url = web_utils.private_rest_url(path_url=CONSTANTS.POSITION_LIST)
        mock_api.get(
            re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + ".*"),
            body=json.dumps(self.funding_payment_mock_response),
        )

        timestamp, unrealized_funding, realized_pnl = self.async_run_with_timeout(
            self.exchange._fetch_last_fee_payment(self.trading_pair)
        )

        self.assertEqual(timestamp, self.target_funding_payment_timestamp)
        self.assertEqual(
            unrealized_funding, Decimal(str(self.target_funding_payment_funding_rate))
        )
        self.assertEqual(
            realized_pnl, Decimal(str(self.target_funding_payment_payment_amount))
        )

    @aioresponses()
    def test_fetch_last_fee_payment_no_position(self, mock_api):
        self._simulate_sub_account_address_set()
        url = web_utils.private_rest_url(path_url=CONSTANTS.POSITION_LIST)
        mock_api.get(
            re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + ".*"),
            body=json.dumps({"list": []}),
        )

        timestamp, unrealized_funding, realized_pnl = self.async_run_with_timeout(
            self.exchange._fetch_last_fee_payment(self.trading_pair)
        )

        self.assertEqual(timestamp, 0)
        self.assertEqual(unrealized_funding, Decimal("0"))
        self.assertEqual(realized_pnl, Decimal("0"))

    @aioresponses()
    def test_place_order_without_sub_account_raises_error(self, mock_api):
        self._simulate_trading_rules_initialized()
        self.exchange._sub_account_address = None

        with self.assertRaises(ValueError) as context:
            self.async_run_with_timeout(
                self.exchange._place_order(
                    order_id="test_order",
                    trading_pair=self.trading_pair,
                    amount=Decimal("1.0"),
                    trade_type=TradeType.BUY,
                    order_type=OrderType.LIMIT,
                    price=Decimal("3000.0"),
                )
            )

        self.assertIn("Vault address not available", str(context.exception))

    @aioresponses()
    def test_cancel_order_without_sub_account_raises_error(self, mock_api):
        order = InFlightOrder(
            client_order_id="test_order",
            exchange_order_id="exch_order",
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("1.0"),
            price=Decimal("3000.0"),
            creation_timestamp=1640780000,
        )
        self.exchange._order_tracker.start_tracking_order(order)
        self.exchange._sub_account_address = None

        with self.assertRaises(ValueError) as context:
            self.async_run_with_timeout(
                self.exchange._place_cancel("test_order", order)
            )

        self.assertIn("Vault address not available", str(context.exception))

    @aioresponses()
    def test_get_price_fallback_to_base_when_mark_price_unavailable(self, mock_api):
        self._simulate_trading_rules_initialized()

        order_book = MagicMock()
        order_book.ask_entries.return_value = iter([])
        order_book.bid_entries.return_value = iter([])
        self.exchange._order_book_tracker.order_books = {self.trading_pair: order_book}

        with patch.object(
            self.exchange,
            "get_funding_info",
            side_effect=Exception("Funding info unavailable"),
        ):
            with patch.object(
                self.exchange.__class__.__bases__[0],
                "get_price",
                return_value=Decimal("3100.0"),
            ) as mock_base_get_price:
                price = self.exchange.get_price(self.trading_pair, True)
                mock_base_get_price.assert_called_once()
                self.assertEqual(price, Decimal("3100.0"))

    @aioresponses()
    def test_format_trading_rules(self, mock_api):
        exchange_info = {
            "list": [
                {
                    "symbol": self.exchange_trading_pair,
                    "mark_price": "3000.0",
                }
            ]
        }

        rules = self.async_run_with_timeout(
            self.exchange._format_trading_rules(exchange_info)
        )

        self.assertEqual(len(rules), 1)
        self.assertEqual(rules[0].trading_pair, self.trading_pair)
        self.assertIsInstance(rules[0], TradingRule)

    @aioresponses()
    def test_initialize_trading_pair_symbols(self, mock_api):
        exchange_info = {
            "list": [
                {
                    "symbol": self.exchange_trading_pair,
                }
            ]
        }

        self.exchange._initialize_trading_pair_symbols_from_exchange_info(exchange_info)

        symbol_map = self.exchange.trading_pair_symbol_map
        self.assertEqual(symbol_map[self.exchange_trading_pair], self.trading_pair)
        self.assertEqual(
            symbol_map.inverse[self.trading_pair], self.exchange_trading_pair
        )

    def test_get_buy_and_sell_collateral_tokens(self):
        self._simulate_trading_rules_initialized()

        buy_collateral = self.exchange.get_buy_collateral_token(self.trading_pair)
        sell_collateral = self.exchange.get_sell_collateral_token(self.trading_pair)

        self.assertEqual(buy_collateral, self.quote_asset)
        self.assertEqual(sell_collateral, self.quote_asset)

    @property
    def all_symbols_including_invalid_pair_mock_response(self):
        return "INVALID-PAIR", {
            "list": [
                {
                    "symbol": "INVALID-PAIR",
                    "mark_price": "3000.0",
                }
            ]
        }

    def exchange_symbol_for_tokens(self, base_token: str, quote_token: str) -> str:
        return f"{base_token}-{quote_token}"

    def create_exchange_instance(self):
        exchange = EkidenPerpetualDerivative(
            aptos_private_key="test_private_key",
            trading_pairs=[self.trading_pair],
            trading_required=True,
        )
        return exchange

    def validate_auth_credentials_present(self, request_call: RequestCall):
        request_headers = request_call.kwargs.get("headers", {})
        self.assertIn("Authorization", request_headers)

    def validate_order_creation_request(
        self, order: InFlightOrder, request_call: RequestCall
    ):
        request_data = json.loads(request_call.kwargs["data"])
        self.assertEqual(self.exchange_trading_pair, request_data["symbol"])
        self.assertEqual(str(order.amount), request_data["qty"])
        self.assertEqual(str(order.price), request_data["price"])
        self.assertEqual(order.client_order_id, request_data["order_link_id"])

    def validate_order_cancelation_request(
        self, order: InFlightOrder, request_call: RequestCall
    ):
        request_data = json.loads(request_call.kwargs["data"])
        self.assertEqual(self.exchange_trading_pair, request_data["symbol"])
        self.assertEqual(order.exchange_order_id, request_data["order_id"])
        self.assertEqual(order.client_order_id, request_data["order_link_id"])

    def validate_order_status_request(
        self, order: InFlightOrder, request_call: RequestCall
    ):
        request_params = request_call.kwargs.get("params", {})
        self.assertEqual(self.exchange_trading_pair, request_params.get("symbol"))
        if order.exchange_order_id:
            self.assertEqual(order.exchange_order_id, request_params.get("order_id"))

    def validate_trades_request(self, order: InFlightOrder, request_call: RequestCall):
        request_params = request_call.kwargs.get("params", {})
        self.assertEqual(self.exchange_trading_pair, request_params.get("symbol"))
        if order.exchange_order_id:
            self.assertEqual(order.exchange_order_id, request_params.get("order_id"))
