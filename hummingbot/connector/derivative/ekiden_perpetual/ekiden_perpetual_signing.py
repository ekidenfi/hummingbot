import hashlib
import json
from typing import Any, Dict

from aptos_sdk.account import Account


def build_intent_payload(payload: Dict[str, Any], nonce: int) -> str:
    msg = {"payload": payload, "nonce": nonce}
    json_bytes = json.dumps(msg, separators=(",", ":"), sort_keys=True).encode("utf-8")
    digest = hashlib.sha3_256(json_bytes).hexdigest()
    return digest


def sign_intent(trading_account: Account, payload: Dict[str, Any], nonce: int) -> str:
    hex_message = build_intent_payload(payload, nonce)
    message_bytes = bytes.fromhex(hex_message)
    sig_hex = trading_account.sign(message_bytes)
    return str(sig_hex)
