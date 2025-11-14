import base64
import json
import secrets
import time
from typing import Optional

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
    def __init__(self, aptos_private_key: str):
        self._root_private_key = aptos_private_key
        self._root_account = Account.load_key(self._root_private_key)
        self.trading_account = None
        self.trading_address = None
        self.pub_key = None
        self._token = None
        self.derive_trading_acc()

    @property
    def auth_token(self) -> Optional[str]:
        if not self._token:
            raise ValueError("Auth token not initialized yet")
        return self._token

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        if self._token is None:
            self._token = await self.get_auth_token()
        request.headers = request.headers or {"Content-Type": "application/json"}
        request.headers["Authorization"] = f"Bearer {self._token}"
        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        return request

    async def get_auth_token(self) -> str:
        url = f"{PERPETUAL_BASE_URL}{API_VERSION}{AUTH_URL}"
        timestamp_ms = int(time.time() * 1000)

        nonce_b64url = (
            base64.urlsafe_b64encode(secrets.token_bytes(16)).decode().rstrip("=")
        )
        message = f"AUTHORIZE|{timestamp_ms}|{nonce_b64url}".encode()
        signature = str(self.trading_account.sign(message))

        payload = {
            "public_key": self.pub_key,
            "timestamp_ms": timestamp_ms,
            "nonce": nonce_b64url,
            "signature": signature,
        }

        async with ClientSession() as session:
            connection = RESTConnection(session)
            request = RESTRequest(
                method=RESTMethod.POST,
                url=url,
                data=json.dumps(payload),
                headers={"Content-Type": "application/json"},
            )
            response = await connection.call(request)
            json_resp = await response.json()

        token = json_resp.get("token")
        return token

    def derive_trading_acc(self, nonce: int = 0) -> None:
        DERIVATION_PREFIX = "APTOS\nmessage: Ekiden Trading\nnonce: "
        root_pub_key = str(self._root_account.account_address)
        msg = f"{DERIVATION_PREFIX}{root_pub_key.lower()}Tradingv2{nonce}"

        sig_hex = str(self._root_account.sign(msg.encode())).replace("0x", "")
        derived_seed32 = bytes.fromhex(sig_hex)[:32]
        derived_pk = f"ed25519-priv-0x{derived_seed32.hex()}"

        self.trading_account = Account.load_key(derived_pk)
        self.trading_address = str(self.trading_account.address)
        self.pub_key = str(self.trading_account.public_key())
