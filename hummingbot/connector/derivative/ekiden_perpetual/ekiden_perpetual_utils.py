import time
from decimal import Decimal
from typing import Tuple

from pydantic import Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0"),
    taker_percent_fee_decimal=Decimal("0.02"),
)

CENTRALIZED = True
EXAMPLE_PAIR = "BTC-WUSDC"


def get_scale_factors(
    trading_rule: "TradingRule", inverse: bool = False
) -> Tuple[Decimal, Decimal]:
    """
    Returns precise Decimal scale factors for price and amount based on trading rule increments.

    Args:
        trading_rule: TradingRule object with min_price_increment and min_base_amount_increment
        inverse: if True, returns the inverse of the scale factors

    Returns:
        Tuple of (price_factor, amount_factor) as Decimals
    """
    price_factor = Decimal("1") / trading_rule.min_price_increment
    amount_factor = Decimal("1") / trading_rule.min_base_amount_increment
    if inverse:
        price_factor = Decimal("1") / price_factor
        amount_factor = Decimal("1") / amount_factor
    return price_factor, amount_factor


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
