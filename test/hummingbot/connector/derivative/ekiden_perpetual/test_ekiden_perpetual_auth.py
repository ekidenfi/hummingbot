import asyncio
import unittest
from typing import Awaitable

from aioresponses import aioresponses

import hummingbot.connector.derivative.ekiden_perpetual.ekiden_perpetual_constants as CONSTANTS
from hummingbot.connector.derivative.ekiden_perpetual.ekiden_perpetual_auth import EkidenPerpetualAuth
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest, WSJSONRequest


class EkidenPerpetualAuthUnitTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.ev_loop = asyncio.get_event_loop()
        cls.aptos_private_key = (
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"  # noqa: mock
        )

    def setUp(self) -> None:
        super().setUp()
        self.auth = EkidenPerpetualAuth(
            aptos_private_key=self.aptos_private_key,
            is_trading_required=True,
        )

    def async_run_with_timeout(self, coroutine: Awaitable, timeout: float = 1):
        ret = self.ev_loop.run_until_complete(asyncio.wait_for(coroutine, timeout))
        return ret

    @aioresponses()
    def test_rest_authenticate(self, mock_api):
        mock_api.post(
            f"{CONSTANTS.PERPETUAL_BASE_URL}{CONSTANTS.API_VERSION}{CONSTANTS.AUTH_URL}",
            payload={"token": "test_token"},
        )

        request = RESTRequest(
            method=RESTMethod.GET,
            url="/test",
        )

        result = self.async_run_with_timeout(self.auth.rest_authenticate(request))

        self.assertIn("Authorization", result.headers)
        self.assertEqual(result.headers["Authorization"], "Bearer test_token")

    def test_ws_authenticate(self):
        request = WSJSONRequest(payload={})

        result = self.async_run_with_timeout(self.auth.ws_authenticate(request))

        self.assertIsNotNone(result)
        self.assertEqual(result, request)

    @aioresponses()
    def test_get_auth_token(self, mock_api):
        mock_api.post(
            f"{CONSTANTS.PERPETUAL_BASE_URL}{CONSTANTS.API_VERSION}{CONSTANTS.AUTH_URL}",
            payload={"token": "test_token"},
        )

        token = self.async_run_with_timeout(self.auth.get_auth_token())

        self.assertEqual(token, "test_token")
        self.assertEqual(self.auth._token, "test_token")

    @aioresponses()
    def test_get_auth_token_no_token_in_response(self, mock_api):
        mock_api.post(
            f"{CONSTANTS.PERPETUAL_BASE_URL}{CONSTANTS.API_VERSION}{CONSTANTS.AUTH_URL}",
            payload={},
        )

        with self.assertRaises(ValueError) as context:
            self.async_run_with_timeout(self.auth.get_auth_token())

        self.assertIn("No token", str(context.exception))

    def test_initialize_account_with_key(self):
        auth = EkidenPerpetualAuth(
            aptos_private_key=self.aptos_private_key,
            is_trading_required=True,
        )

        self.assertIsNotNone(auth._root_account)

    def test_initialize_account_without_key_trading_required(self):
        with self.assertRaises(ValueError) as context:
            EkidenPerpetualAuth(
                aptos_private_key="",
                is_trading_required=True,
            )

        self.assertIn("Need a real private key", str(context.exception))

    def test_derive_trading_acc(self):
        trading_account, trading_address = self.auth.derive_trading_acc()

        self.assertIsNotNone(trading_account)
        self.assertIsNotNone(trading_address)
        self.assertIsInstance(trading_address, str)
