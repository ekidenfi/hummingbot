import asyncio
import time
from typing import Any, Dict, Optional

from hummingbot.connector.derivative.ekiden_perpetual import ekiden_perpetual_constants as CONSTANTS
from hummingbot.connector.derivative.ekiden_perpetual.ekiden_perpetual_auth import EkidenPerpetualAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.tracking_nonce import NonceCreator
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger


class EkidenPerpetualUserStreamDataSource(UserStreamTrackerDataSource):
    _logger: Optional[HummingbotLogger] = None

    def __init__(
        self,
        auth: EkidenPerpetualAuth,
        api_factory: WebAssistantsFactory,
        domain: str = CONSTANTS.DOMAIN,
    ):
        super().__init__()
        self._domain = domain
        self._api_factory = api_factory
        self._auth = auth
        self._nonce_provider = NonceCreator.for_microseconds()

    async def listen_for_user_stream(self, output: asyncio.Queue):
        """
        Connects to the user private channel in the exchange using a websocket connection. With the established
        connection listens to all balance events and order updates provided by the exchange, and stores them in the
        output queue
        :param output: the queue to use to store the received messages
        """
        while True:
            try:
                self._ws_assistant = await self._connected_websocket_assistant()
                await self._subscribe_channels(self._ws_assistant)
                self._last_ws_message_sent_timestamp = time.time()
                while True:
                    try:
                        seconds_until_next_ping = CONSTANTS.HEARTBEAT_TIME_INTERVAL - (
                            self._time() - self._last_ws_message_sent_timestamp
                        )
                        await asyncio.wait_for(
                            self._process_websocket_messages(
                                websocket_assistant=self._ws_assistant, queue=output
                            ),
                            timeout=seconds_until_next_ping,
                        )
                    except asyncio.TimeoutError:
                        await self._ping_server(self._ws_assistant)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception(
                    "Unexpected error while listening to user stream. Retrying after 5 seconds..."
                )
            finally:
                if self._ws_assistant:
                    await self._ws_assistant.disconnect()
                await self._sleep(5)

    async def _ping_server(self, ws: WSAssistant):
        ping_time = self._time()
        payload = {
            "op": "ping",
            "ts": int(ping_time * 1e3),
            "req_id": f"{self._nonce_provider.get_tracking_nonce()}",
        }
        ping_request = WSJSONRequest(payload=payload)
        await ws.send(request=ping_request)
        self._last_ws_message_sent_timestamp = ping_time

    async def _subscribe_channels(self, websocket_assistant: WSAssistant):
        """
        Subscribes to the trade events and diff orders events through the provided websocket connection.
        :param ws: the websocket assistant used to connect to the exchange
        """
        try:
            orders_payload = {
                "op": "subscribe",
                "args": ["order"],
                "req_id": f"{self._nonce_provider.get_tracking_nonce()}",
            }
            subscribe_orders_request: WSJSONRequest = WSJSONRequest(
                payload=orders_payload
            )
            positions_payload = {
                "op": "subscribe",
                "args": ["position"],
                "req_id": f"{self._nonce_provider.get_tracking_nonce()}",
            }
            subscribe_positions_request: WSJSONRequest = WSJSONRequest(
                payload=positions_payload
            )
            fills_payload = {
                "op": "subscribe",
                "args": ["fill"],
                "req_id": f"{self._nonce_provider.get_tracking_nonce()}",
            }
            subscribe_fills_request: WSJSONRequest = WSJSONRequest(
                payload=fills_payload
            )
            await websocket_assistant.send(subscribe_orders_request)
            await websocket_assistant.send(subscribe_positions_request)
            await websocket_assistant.send(subscribe_fills_request)
            self.logger().info(
                "Subscribed to private orders, trades and fills channels..."
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception(
                "Unexpected error occurred subscribing to user streams..."
            )
            raise

    async def _process_event_message(
        self, event_message: Dict[str, Any], queue: asyncio.Queue
    ):
        op = event_message.get("op")
        match op:
            case "auth":
                await self._process_ws_auth_msg(event_message)
            case "subscribed":
                topic = event_message.get("args", "unknown")
            case "pong":
                pass
            case "event":
                topic = event_message.get("topic")
                if topic in CONSTANTS.PRIVATE_TOPICS:
                    queue.put_nowait(event_message)
            case "error":
                msg = event_message.get("message", "Unknown error")
                self.logger().error(f"Error from user stream: , msg={msg}")
                raise IOError(msg)
            case _:
                self.logger().warning(f"Unrecognized user stream ws op: {op}")

    async def _process_ws_auth_msg(self, data: dict):
        success = data.get("success", False)
        if success:
            self.logger().info("WebSocket authentication succeeded.")
        else:
            error = data.get("message", "Unknown error")
            self.logger().error(f"WebSocket authentication failed: {error}")
            raise IOError(error)

    async def _get_ws_assistant(self) -> WSAssistant:
        if self._ws_assistant is None:
            self._ws_assistant = await self._api_factory.get_ws_assistant()
        return self._ws_assistant

    async def _connected_websocket_assistant(self) -> WSAssistant:
        """
        Creates an instance of WSAssistant connected to the exchange
        """
        ws: WSAssistant = await self._get_ws_assistant()
        url = f"{CONSTANTS.PERPETUAL_WS_URL}{CONSTANTS.WS_PRIVATE}"
        await ws.connect(ws_url=url, ping_timeout=CONSTANTS.HEARTBEAT_TIME_INTERVAL)
        await self._authenticate_connection(ws)
        return ws

    async def _authenticate_connection(self, ws: WSAssistant):
        """
        Sends the authentication message.
        :param ws: the websocket assistant used to connect to the exchange
        """
        token = await self._auth.get_auth_token()
        auth_message = {
            "op": "auth",
            "bearer": token,
            "req_id": f"{self._nonce_provider.get_tracking_nonce()}",
        }
        auth_request: WSJSONRequest = WSJSONRequest(payload=auth_message)
        await ws.send(auth_request)
