from decimal import Decimal
from typing import Any, Dict

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


def is_exchange_information_valid(rule: Dict[str, Any]) -> bool:
    """
    Verifies if a trading pair is enabled to operate with based on its exchange information
    :param exchange_info: the exchange information for a trading pair
    :return: True if the trading pair is enabled, False otherwise
    """
    return True


class EkidenPerpetualConfigMap(BaseConnectorConfigMap):
    connector: str = "ekiden_perpetual"
    aptos_private_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Aptos wallet private key",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )


KEYS = EkidenPerpetualConfigMap.model_construct()
