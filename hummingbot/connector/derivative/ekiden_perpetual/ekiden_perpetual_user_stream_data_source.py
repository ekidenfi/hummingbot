import asyncio
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import hummingbot.connector.derivative.ekiden_perpetual.ekiden_perpetual_constants as CONSTANTS
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.derivative.ekiden_perpetual.ekiden_perpetual_derivative import EkidenPerpetualDerivative


class EkidenPerpetualUserStreamDataSource(UserStreamTrackerDataSource):
    _logger: Optional[HummingbotLogger] = None

    def __init__(
        self,
        api_factory: WebAssistantsFactory,
        auth: AuthBase,
        connector: "EkidenPerpetualDerivative",
        domain: str = CONSTANTS.DOMAIN,
    ):
        super().__init__()
        self._api_factory = api_factory
        self._auth = auth
        self._connector = connector
        self._ws_assistants: List[WSAssistant] = []
        self.domain = domain

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
        safe_ensure_future(self._send_ping(ws))
        return ws

    async def _subscribe_channels(self, websocket_assistant: WSAssistant):
        """
        Subscribes to order events.

        :param websocket_assistant: the websocket assistant used to connect to the exchange
        """
        try:
            id = int(time.time())
            auth_message = {
                "op": "auth",
                "bearer": self._auth.auth_token,
                "req_id": f"{id}",
            }
            auth_request: WSJSONRequest = WSJSONRequest(payload=auth_message)

            orders_change_payload = {
                "op": "subscribe",
                "args": ["order"],
                "req_id": f"{id + 1}",
            }
            subscribe_order_change_request: WSJSONRequest = WSJSONRequest(
                payload=orders_change_payload
            )

            positions_payload = {
                "op": "subscribe",
                "args": ["position"],
                "req_id": {"id + 2"},
            }
            subscribe_positions_request: WSJSONRequest = WSJSONRequest(
                payload=positions_payload
            )

            await websocket_assistant.send(auth_request)
            await websocket_assistant.send(subscribe_order_change_request)
            await websocket_assistant.send(subscribe_positions_request)

            self.logger().info(
                "Subscribed to private order and trades changes channels..."
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
        logger = self.logger()
        op = event_message.get("op")
        match op:
            case "auth":
                success = event_message.get("success", False)
                if success:
                    logger.info("WebSocket authentication succeeded.")
                else:
                    error = event_message.get("message", "Unknown error")
                    logger.error(f"WebSocket authentication failed: {error}")
            case "subscribed":
                topic = event_message.get("args", "unknown")
                logger.info(f"Subscribed to topic: {topic}")
            case "pong":
                logger.debug("Received pong message (heartbeat).")
            case "event":
                topic = event_message.get("topic")
                if topic in CONSTANTS.PRIVATE_TOPICS:
                    queue.put_nowait(event_message)
            case "error":
                msg = event_message.get("message", "Unknown error")
                logger.error(f"Error from user stream: , msg={msg}")
            case _:
                logger.warning(f"Unrecognized user stream ws op: {op}")

    async def _send_ping(
        self,
        websocket_assistant: WSAssistant,
    ):
        try:
            while True:
                ping_request = WSJSONRequest(payload={"op": "ping"})
                await asyncio.sleep(CONSTANTS.HEARTBEAT_TIME_INTERVAL)
                await websocket_assistant.send(ping_request)
        except Exception as e:
            self.logger().debug(f"ping error {e}")

    async def _process_websocket_messages(
        self, websocket_assistant: WSAssistant, queue: asyncio.Queue
    ):
        while True:
            try:
                await super()._process_websocket_messages(
                    websocket_assistant=websocket_assistant, queue=queue
                )
            except asyncio.TimeoutError:
                ping_request = WSJSONRequest(payload={"op": "ping"})
                await websocket_assistant.send(ping_request)
