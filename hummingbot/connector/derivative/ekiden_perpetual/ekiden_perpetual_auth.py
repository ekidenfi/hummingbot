import base64
import json
import secrets
import time
from typing import Optional, Tuple

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
    def __init__(self, aptos_private_key: str, is_trading_required: bool):
        self._root_account = self.initialize_account(
            aptos_private_key, is_trading_required
        )
        self.trading_account, self.trading_address = self.derive_trading_acc()
        self._token: Optional[str] = None

    # TODO: Add auth error handling and token refetching
    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        if self._token is None:
            await self.get_auth_token()
        request.headers = request.headers or {"Content-Type": "application/json"}
        request.headers.update({"Authorization": f"Bearer {self._token}"})
        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        return request

    async def get_auth_token(self) -> str:
        if self._token:
            return self._token
        url = f"{PERPETUAL_BASE_URL}{API_VERSION}{AUTH_URL}"
        timestamp_ms = int(time.time() * 1000)

        nonce_b64url = (
            base64.urlsafe_b64encode(secrets.token_bytes(16)).decode().rstrip("=")
        )
        full_message = f"APTOS\nmessage: AUTHORIZE|{timestamp_ms}|{nonce_b64url}\nnonce: {nonce_b64url}"
        signature = str(self._root_account.sign(full_message.encode()))

        payload = {
            "full_message": full_message,
            "nonce": nonce_b64url,
            "public_key": str(self._root_account.public_key()),
            "signature": signature,
            "timestamp_ms": timestamp_ms,
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

        token: str = json_resp.get("token")
        if not token:
            raise ValueError("No token in ekiden's auth response")
        self._token = token
        return self._token

    def initialize_account(
        self, aptos_private_key: str, is_trading_required: bool
    ) -> Account:
        match bool(aptos_private_key), is_trading_required:
            case False, False:
                return Account.generate()
            case False, True:
                raise ValueError("Need a real private key for the trading")
            case _:
                return Account.load_key(aptos_private_key)

    def derive_trading_acc(self, nonce: int = 0) -> Tuple[Account, str]:
        DERIVATION_PREFIX = "APTOS\nmessage: Ekiden Trading\nnonce: "
        root_addr = str(self._root_account.account_address)
        msg = f"{DERIVATION_PREFIX}{root_addr.lower()}Tradingv2{nonce}"

        sig_hex = str(self._root_account.sign(msg.encode())).replace("0x", "")
        derived_seed32 = bytes.fromhex(sig_hex)[:32]
        derived_pk = f"ed25519-priv-0x{derived_seed32.hex()}"

        trading_account = Account.load_key(derived_pk)
        trading_address = str(trading_account.account_address)
        return (trading_account, trading_address)
