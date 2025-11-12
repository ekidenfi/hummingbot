import base64
import json
import secrets
import time

from aiohttp import ClientSession
from aptos_sdk.account import Account

from hummingbot.connector.derivative.ekiden_perpetual.ekiden_perpetual_constants import (
    API_VERSION,
    AUTH_URL,
    PERPETUAL_BASE_URL,
)
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest, WSRequest
from hummingbot.core.web_assistant.connections.rest_connection import RESTConnection


class EkidenPerpetualAuth(AuthBase):
    """
    Auth class required by Ekiden Perpetual API
    """

    def __init__(self, private_key: str):
        self._private_key = private_key
        self._token = None
        self._init_wallet()

    def _init_wallet(self):
        account = Account.load_key(self._private_key)
        self._public_key = account.account_address.__str__()
        self._signing_key = account.private_key

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        if getattr(request, "is_auth_required", False) and not self._token:
            await self.get_auth_token()

        if getattr(request, "is_auth_required", False):
            request.headers = request.headers or {}
            request.headers["Authorization"] = f"Bearer {self._token}"

        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        return request

    async def get_auth_token(self):
        url = f"{PERPETUAL_BASE_URL}{API_VERSION}{AUTH_URL}"

        timestamp_ms = int(time.time() * 1000)

        nonce_bytes = secrets.token_bytes(16)
        nonce_b64url = base64.urlsafe_b64encode(nonce_bytes).decode().rstrip("=")

        message = f"AUTHORIZE|{timestamp_ms}|{nonce_b64url}".encode("utf-8")
        signature = self._signing_key.sign(message).to_bytes().hex()

        payload = {
            "public_key": self._public_key,
            "timestamp_ms": timestamp_ms,
            "nonce": nonce_b64url,
            "signature": signature,
        }
        headers = {"Content-type: application/json"}

        request = RESTRequest(
            method=RESTMethod.POST,
            url=url,
            data=json.dumps(payload),
            headers=headers
        )

        session = ClientSession()
        connection = RESTConnection(session)
        response = await connection.call(request)
        json_resp = await response.json()

        self._token = json_resp.get("token")
        await session.close()
        return self._token
