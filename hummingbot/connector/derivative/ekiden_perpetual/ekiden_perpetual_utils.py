from decimal import Decimal

from pydantic import Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0"),
    taker_percent_fee_decimal=Decimal("0.02"),
    buy_percent_fee_deducted_from_returns=True,
)

CENTRALIZED = True
EXAMPLE_PAIR = "BTC-WUSDC"


class EkidenPerpetualConfigMap(BaseConnectorConfigMap):
    connector: str = "ekiden_perpetual"
    ekiden_perpetual_private_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Aptos wallet private key",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )


KEYS = EkidenPerpetualConfigMap.model_construct()
