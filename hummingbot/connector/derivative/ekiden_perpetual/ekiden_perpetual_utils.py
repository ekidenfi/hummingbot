import time
from decimal import Decimal

from pydantic import Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0"),
    taker_percent_fee_decimal=Decimal("0.02"),
)

CENTRALIZED = True
EXAMPLE_PAIR = "BTC-WUSDC"


def get_funding_timestamp(switch: bool = True) -> int:
    """
    Get funding timestamp (next funding or last one)
    :param switch: selects next funding time or last

    Funding settlement occurs every 1 hour as mentioned in https://ekiden.gitbook.io/ekiden-docs/trading/funding
    """
    next_or_last = 1 if switch else -1
    return int(((time.time() // 3600) + next_or_last) * 3600 * 1e3)


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
