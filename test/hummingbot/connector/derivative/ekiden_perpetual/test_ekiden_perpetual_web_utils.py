import asyncio
import unittest
from typing import Awaitable

import hummingbot.connector.derivative.ekiden_perpetual.ekiden_perpetual_constants as CONSTANTS
import hummingbot.connector.derivative.ekiden_perpetual.ekiden_perpetual_web_utils as web_utils
from hummingbot.connector.derivative.ekiden_perpetual.ekiden_perpetual_web_utils import HeadersContentRESTPreProcessor
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest


class EkidenPerpetualWebUtilsUnitTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.ev_loop = asyncio.get_event_loop()
        cls.pre_processor = HeadersContentRESTPreProcessor()

    def async_run_with_timeout(self, coroutine: Awaitable, timeout: float = 1):
        ret = self.ev_loop.run_until_complete(asyncio.wait_for(coroutine, timeout))
        return ret

    def test_rest_pre_processor(self):
        request = RESTRequest(
            method=RESTMethod.GET,
            url="/test",
        )

        result = self.async_run_with_timeout(self.pre_processor.pre_process(request))

        self.assertIn("Content-Type", result.headers)
        self.assertEqual(result.headers["Content-Type"], "application/json")

    def test_public_rest_url(self):
        url = web_utils.public_rest_url(path_url="/test")

        expected_url = f"{CONSTANTS.PERPETUAL_BASE_URL}{CONSTANTS.API_VERSION}/test"
        self.assertEqual(url, expected_url)

    def test_private_rest_url(self):
        url = web_utils.private_rest_url(path_url="/test")

        expected_url = f"{CONSTANTS.PERPETUAL_BASE_URL}{CONSTANTS.API_VERSION}/test"
        self.assertEqual(url, expected_url)

    def test_build_api_factory(self):
        factory = web_utils.build_api_factory()

        self.assertIsNotNone(factory)
        self.assertIsNotNone(factory._throttler)

    def test_create_throttler(self):
        throttler = web_utils.create_throttler()

        self.assertIsNotNone(throttler)
