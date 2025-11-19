from typing import Any, Dict, Optional

from aptos_sdk.account import Account
from aptos_sdk.bcs import Serializer

from hummingbot.connector.derivative.ekiden_perpetual.ekiden_perpetual_constants import (
    SEED,
    IntentType,
    TimeInForce,
    TpSlMode,
    TpSlOrderType,
)


def encode_intent(payload: Dict[str, Any], nonce: int) -> bytes:
    ser = EkidenIntentSerializer()
    ser.encode(payload)
    payload_bytes = ser.output()
    nonce_bytes = ser._serialize_nonce(nonce)
    seed_bytes = bytes.fromhex(SEED)
    result = seed_bytes + payload_bytes + nonce_bytes
    return result


def sign_intent(trading_account: Account, payload: Dict, nonce: int) -> str:
    msg = encode_intent(payload, nonce)
    sig = trading_account.sign(msg)
    return str(sig)


class EkidenIntentSerializer:
    def __init__(self):
        self.ser = Serializer()

    def output(self) -> bytes:
        return self.ser.output()

    def option_string(self, val: Optional[str]):
        if val is None:
            self.ser.u8(0)
        else:
            self.ser.u8(1)
            self.ser.str(val)

    def option_string_skip(self, val: Optional[str]):
        if val is None:
            return
        self.ser.u8(1)
        self.ser.str(val)

    def option_u64(self, val: Optional[int]):
        if val is None:
            self.ser.u8(0)
        else:
            self.ser.u8(1)
            self.ser.u64(int(val))

    def option_bool(self, val: Optional[bool]):
        if val is None:
            self.ser.u8(0)
        else:
            self.ser.u8(1)
            self.ser.bool(bool(val))

    def option_time_in_force(self, val: Optional[str]):
        if val is None:
            self.ser.u8(0)
        else:
            self.ser.u8(1)
            self.ser.u8(self.encode_time_in_force(val))

    def option_u64_skip(self, val: Optional[int]):
        if val is None:
            return
        self.ser.u8(1)
        self.ser.u64(int(val))

    def option_bool_skip(self, val: Optional[bool]):
        if val is None:
            return
        self.ser.u8(1)
        self.ser.bool(bool(val))

    def encode(self, payload: Dict[str, Any]):
        action_type_str: Optional[str] = payload.get("type", None)

        if not isinstance(action_type_str, str):
            raise ValueError(f"Unknown action type: {action_type_str}")

        try:
            action_type = IntentType(action_type_str)
        except ValueError:
            raise ValueError(f"Unknown action type: {action_type_str}")

        self.ser.str(action_type.value)

        match action_type:
            case IntentType.ORDER_CREATE:
                self._encode_order_create(payload)
            case IntentType.ORDER_CANCEL:
                self._encode_order_cancel(payload)
            case IntentType.ORDER_CANCEL_ALL:
                self._encode_order_cancel_all(payload)
            case IntentType.LEVERAGE_ASSIGN:
                self._encode_leverage_assign(payload)

    def _encode_leverage_assign(self, p: Dict[str, Any]):
        self.ser.u64(int(p["leverage"]))
        self.ser.str(p["market_addr"])

    def _encode_order_cancel(self, p: Dict[str, Any]):
        cancels = p["cancels"]
        self.ser.uleb128(len(cancels))
        for c in cancels:
            self.ser.str(c["sid"])

    def _encode_order_cancel_all(self, p: Dict[str, Any]):
        self.option_string(p.get("market_addr"))

    def _encode_order_create(self, p: Dict[str, Any]):
        orders = p["orders"]
        self.ser.uleb128(len(orders))

        for o in orders:
            self.ser.str(o["side"])
            self.ser.u64(int(o["size"]))
            self.ser.u64(int(o["price"]))
            self.ser.u64(int(o["leverage"]))
            self.ser.str(o["type"])
            self.ser.str(o["market_addr"])
            self.ser.bool(o["is_cross"])

            self.option_string_skip(o.get("time_in_force"))

            self.option_bool_skip(o.get("reduce_only"))

            self.option_string_skip(o.get("order_link_id"))

            bracket = o.get("bracket")
            if bracket is not None:
                self.ser.u8(1)
                self._encode_bracket(bracket)

    def _encode_bracket(self, b: Dict[str, Any]):
        self.ser.uleb128(self.encode_tp_sl_mode(b["mode"]))
        tp = b.get("take_profit")
        sl = b.get("stop_loss")
        if tp is None:
            self.ser.u8(0)
        else:
            self.ser.u8(1)
            self._encode_tp_sl_spec(tp)
        if sl is None:
            self.ser.u8(0)
        else:
            self.ser.u8(1)
            self._encode_tp_sl_spec(sl)

    def _encode_tp_sl_spec(self, spec: Dict[str, Any]):
        self.ser.u64(int(spec["trigger_price"]))
        self.ser.uleb128(self.encode_tp_sl_order_type(spec["order_type"]))
        limit_price = spec.get("limit_price")
        if limit_price is None:
            self.ser.u8(0)
        else:
            self.ser.u8(1)
            self.ser.u64(int(limit_price))

    @staticmethod
    def encode_time_in_force(tif: str) -> int:
        try:
            tif_enum = TimeInForce(tif)
        except ValueError:
            raise ValueError(f"Unknown TimeInForce: {tif}")

        if tif_enum == TimeInForce.GTC:
            return 0
        elif tif_enum == TimeInForce.IOC:
            return 1
        elif tif_enum == TimeInForce.FOK:
            return 2
        elif tif_enum == TimeInForce.POST_ONLY:
            return 3
        else:
            raise ValueError(f"Unknown TimeInForce: {tif}")

    @staticmethod
    def encode_tp_sl_mode(mode: str) -> int:
        try:
            mode_enum = TpSlMode(mode)
        except ValueError:
            raise ValueError(f"Unknown TpSlMode: {mode}")

        if mode_enum == TpSlMode.FULL:
            return 0
        if mode_enum == TpSlMode.PARTIAL:
            return 1
        raise ValueError(f"Unknown TpSlMode: {mode}")

    @staticmethod
    def encode_tp_sl_order_type(order_type: str) -> int:
        try:
            order_type_enum = TpSlOrderType(order_type)
        except ValueError:
            raise ValueError(f"Unknown TpSlOrderType: {order_type}")

        if order_type_enum == TpSlOrderType.MARKET:
            return 0
        if order_type_enum == TpSlOrderType.LIMIT:
            return 1
        raise ValueError(f"Unknown TpSlOrderType: {order_type}")

    @staticmethod
    def _serialize_nonce(nonce: int) -> bytes:
        s = Serializer()
        s.u64(nonce)
        return s.output()
