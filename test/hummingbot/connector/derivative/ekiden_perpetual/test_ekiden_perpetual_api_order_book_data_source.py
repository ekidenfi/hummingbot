import re
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import patch

from aioresponses import aioresponses
from bidict import bidict

import hummingbot.connector.derivative.ekiden_perpetual.ekiden_perpetual_constants as CONSTANTS
import hummingbot.connector.derivative.ekiden_perpetual.ekiden_perpetual_web_utils as web_utils
from hummingbot.connector.derivative.ekiden_perpetual.ekiden_perpetual_api_order_book_data_source import (
    EkidenPerpetualAPIOrderBookDataSource,
)
from hummingbot.connector.derivative.ekiden_perpetual.ekiden_perpetual_derivative import EkidenPerpetualDerivative
from hummingbot.core.data_type.funding_info import FundingInfo


class EkidenPerpetualAPIOrderBookDataSourceUnitTests(IsolatedAsyncioWrapperTestCase):
    level = 0

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.base_asset = "ETH"
        cls.quote_asset = "USDC"
        cls.trading_pair = f"{cls.base_asset}-{cls.quote_asset}"
        cls.exchange_trading_pair = f"{cls.base_asset}-{cls.quote_asset}"

    async def asyncSetUp(self) -> None:
        self.connector = EkidenPerpetualDerivative(
            aptos_private_key="0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",  # noqa: mock
            trading_pairs=[self.trading_pair],
            trading_required=False,
        )
        self.connector._set_trading_pair_symbol_map(
            bidict({self.exchange_trading_pair: self.trading_pair})
        )
        self.data_source = EkidenPerpetualAPIOrderBookDataSource(
            trading_pairs=[self.trading_pair],
            connector=self.connector,
            api_factory=self.connector._web_assistants_factory,
        )

        EkidenPerpetualAPIOrderBookDataSource._trading_pair_symbol_map = {
            CONSTANTS.DOMAIN: bidict({self.exchange_trading_pair: self.trading_pair})
        }

    def tearDown(self) -> None:
        EkidenPerpetualAPIOrderBookDataSource._trading_pair_symbol_map = {}
        super().tearDown()

    @aioresponses()
    async def test_get_funding_info(self, mock_api):
        url = web_utils.public_rest_url(path_url=CONSTANTS.MARKET_STATS)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + ".*")
        mock_api.get(
            regex_url,
            payload={
                "list": [
                    {
                        "symbol": self.exchange_trading_pair,
                        "index_price": "2999.5",
                        "mark_price": "3000.0",
                        "next_funding_time": 1657099053,
                        "funding_rate": "0.0001",
                    }
                ]
            },
        )

        funding_info = await self.data_source.get_funding_info(self.trading_pair)

        self.assertIsInstance(funding_info, FundingInfo)
        self.assertEqual(funding_info.trading_pair, self.trading_pair)
        self.assertEqual(funding_info.index_price, Decimal("2999.5"))
        self.assertEqual(funding_info.mark_price, Decimal("3000.0"))
        self.assertEqual(funding_info.rate, Decimal("0.0001"))

    @aioresponses()
    async def test_get_funding_info_no_ticker(self, mock_api):
        url = web_utils.public_rest_url(path_url=CONSTANTS.MARKET_STATS)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + ".*")
        mock_api.get(
            regex_url,
            payload={"list": []},
        )

        with self.assertRaises(ValueError) as context:
            await self.data_source.get_funding_info(self.trading_pair)

        self.assertIn("No ticker found", str(context.exception))

    async def test_get_last_traded_prices(self):
        with patch.object(
            self.connector,
            "get_last_traded_prices",
            return_value={self.trading_pair: 3000.0},
        ) as mock_get:
            result = await self.data_source.get_last_traded_prices([self.trading_pair])

            mock_get.assert_called_once_with(trading_pairs=[self.trading_pair])
            self.assertEqual(result[self.trading_pair], 3000.0)
