import unittest

from hummingbot.connector.derivative.ekiden_perpetual.ekiden_perpetual_utils import get_funding_timestamp


class EkidenPerpetualUtilsUnitTests(unittest.TestCase):
    def test_get_funding_timestamp(self):
        timestamp = get_funding_timestamp(switch=False)

        self.assertIsInstance(timestamp, int)
        self.assertGreater(timestamp, 0)

    def test_get_funding_timestamp_with_switch(self):
        timestamp = get_funding_timestamp(switch=True)

        self.assertIsInstance(timestamp, int)
        self.assertGreater(timestamp, 0)
