import asyncio
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock, patch

import hummingbot.connector.derivative.ekiden_perpetual.ekiden_perpetual_web_utils as web_utils
from hummingbot.connector.derivative.ekiden_perpetual.ekiden_perpetual_auth import EkidenPerpetualAuth
from hummingbot.connector.derivative.ekiden_perpetual.ekiden_perpetual_user_stream_data_source import (
    EkidenPerpetualUserStreamDataSource,
)


class EkidenPerpetualUserStreamDataSourceUnitTests(IsolatedAsyncioWrapperTestCase):
    level = 0

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.aptos_private_key = (
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"  # noqa: mock
        )

    async def asyncSetUp(self) -> None:
        self.auth = EkidenPerpetualAuth(
            aptos_private_key=self.aptos_private_key,
            is_trading_required=False,
        )
        self.api_factory = web_utils.build_api_factory(auth=self.auth)
        self.data_source = EkidenPerpetualUserStreamDataSource(
            auth=self.auth,
            api_factory=self.api_factory,
        )

    async def test_listen_for_user_stream(self):
        mock_ws = AsyncMock()
        mock_ws.receive.return_value = '{"op": "subscribed"}'
        mock_ws.send = AsyncMock()

        with patch.object(
            self.data_source, "_connected_websocket_assistant", return_value=mock_ws
        ) as mock_connect:
            with patch.object(
                self.data_source, "_subscribe_channels", return_value=None
            ) as mock_subscribe:
                with patch.object(
                    self.data_source,
                    "_process_websocket_messages",
                    side_effect=asyncio.CancelledError,
                ):
                    output_queue = asyncio.Queue()

                    with self.assertRaises(asyncio.CancelledError):
                        await self.data_source.listen_for_user_stream(output_queue)

                    mock_connect.assert_called_once()
                    mock_subscribe.assert_called_once()

    async def test_subscribe_channels(self):
        mock_ws = AsyncMock()
        mock_ws.send = AsyncMock()

        await self.data_source._subscribe_channels(mock_ws)

        self.assertEqual(mock_ws.send.call_count, 3)

    async def test_connected_websocket_assistant(self):
        mock_ws = AsyncMock()
        mock_ws.connect = AsyncMock()

        with patch.object(
            self.data_source, "_get_ws_assistant", return_value=mock_ws
        ) as _:
            with patch.object(
                self.data_source, "_authenticate_connection", return_value=None
            ) as mock_auth:
                result = await self.data_source._connected_websocket_assistant()

                self.assertEqual(result, mock_ws)
                mock_ws.connect.assert_called_once()
                mock_auth.assert_called_once_with(mock_ws)
