from enum import Enum

from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit
from hummingbot.core.data_type.in_flight_order import OrderState

DOMAIN = ""

EXCHANGE_NAME = "ekiden_perpetual"
PERPETUAL_BASE_URL = "https://api.staging.ekiden.fi"
PERPETUAL_WS_URL = "wss://api.staging.ekiden.fi/ws"

BROKER_ID = "HBOT"

WS_PUBLIC = "/public"
WS_PRIVATE = "/private"

API_VERSION = "/api/v1"

PING_URL = "/ping"

MARKET_STATS = "/market/candles/stats/{market_addr}"
MARKET_FILLS = "/market/fills"
MARKET_FUNDING = "/market/funding_rate"
MARKET_INFO = "/market/market_info"
MARKET_ORDERS = "/market/orders"

DEPOSITS = "/transaction/deposits"
WITHDRAWALS = "/transaction/withdrawals"

AUTH_URL = "/authorize"

USER_SUBACCOUNTS = "/user/accounts/owned"
USER_ACCOUNT_OWNER = "/user/accounts/owner"
USER_FILLS = "/user/fills"
USER_SEND_INTENT = "/user/intent"
USER_SET_LEVERAGE = "/user/leverage"
USER_ORDERS = "/user/orders"
USER_PORTFOLIO = "/user/portfolio"
USER_POSITIONS = "/user/positions"
USER_VAULTS = "/user/vaults"
USER_WITHDRAW_TO_FUNDING = "/user/vaults/withdraw"


class IntentType(Enum):
    ORDER_CREATE = "order_create"
    ORDER_CANCEL = "order_cancel"
    ORDER_CANCEL_ALL = "order_cancel_all"
    LEVERAGE_ASSIGN = "leverage_assign"


class TimeInForce(Enum):
    GTC = "GTC"
    IOC = "IOC"
    FOK = "FOK"
    POST_ONLY = "PostOnly"


class TpSlMode(Enum):
    FULL = "FULL"
    PARTIAL = "PARTIAL"


class TpSlOrderType(Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"


class OrderSide(Enum):
    BUY = "buy"
    SELL = "sell"


class OrderTypeString(Enum):
    LIMIT = "limit"
    MARKET = "market"


ORDER_STATUSES = {
    "created": OrderState.OPEN,
    "placed": OrderState.OPEN,
    "filled": OrderState.FILLED,
    "cancelled": OrderState.CANCELED,
    "rejected": OrderState.FAILED,
    "partial_filled": OrderState.PARTIALLY_FILLED,
    "partial_filled_and_cancelled": OrderState.PARTIALLY_FILLED,
}


WS_TRADES = "trade"
WS_ORDERBOOK = "orderbook"
WS_TICKER = "ticker"
PUBLIC_TOPICS = [WS_TRADES, WS_ORDERBOOK, WS_TICKER]

WS_USER_ORDER = "order"
WS_USER_POSITION = "position"
WS_USER_FILL = "fill"
PRIVATE_TOPICS = [WS_USER_ORDER, WS_USER_POSITION, WS_USER_FILL]

AUTH_ERROR = {"code": "UNAUTHORIZED", "message": "Unauthorized"}

HEARTBEAT_TIME_INTERVAL = 20.0
FUNDING_INTERVAL_SECONDS = 3600

MAX_REQUEST = 600
ALL_ENDPOINTS_LIMIT = "All"

RATE_LIMITS = [
    RateLimit(ALL_ENDPOINTS_LIMIT, limit=MAX_REQUEST, time_interval=60),
    # Health & Market
    RateLimit(
        PING_URL,
        limit=MAX_REQUEST,
        time_interval=60,
        linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT, 1)],
    ),
    RateLimit(
        MARKET_FILLS,
        limit=MAX_REQUEST,
        time_interval=60,
        linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT, 1)],
    ),
    RateLimit(
        MARKET_FUNDING,
        limit=MAX_REQUEST,
        time_interval=60,
        linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT, 1)],
    ),
    RateLimit(
        MARKET_INFO,
        limit=MAX_REQUEST,
        time_interval=60,
        linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT, 1)],
    ),
    RateLimit(
        MARKET_ORDERS,
        limit=MAX_REQUEST,
        time_interval=60,
        linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT, 1)],
    ),
    # Transactions
    RateLimit(
        DEPOSITS,
        limit=MAX_REQUEST,
        time_interval=60,
        linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT, 1)],
    ),
    RateLimit(
        WITHDRAWALS,
        limit=MAX_REQUEST,
        time_interval=60,
        linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT, 1)],
    ),
    # Auth
    RateLimit(
        AUTH_URL,
        limit=MAX_REQUEST,
        time_interval=60,
        linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT, 1)],
    ),
    # User endpoints
    RateLimit(
        USER_SUBACCOUNTS,
        limit=MAX_REQUEST,
        time_interval=60,
        linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT, 1)],
    ),
    RateLimit(
        USER_ACCOUNT_OWNER,
        limit=MAX_REQUEST,
        time_interval=60,
        linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT, 1)],
    ),
    RateLimit(
        USER_FILLS,
        limit=MAX_REQUEST,
        time_interval=60,
        linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT, 1)],
    ),
    RateLimit(
        USER_SEND_INTENT,
        limit=MAX_REQUEST,
        time_interval=60,
        linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT, 1)],
    ),
    RateLimit(
        USER_SET_LEVERAGE,
        limit=MAX_REQUEST,
        time_interval=60,
        linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT, 1)],
    ),
    RateLimit(
        USER_ORDERS,
        limit=MAX_REQUEST,
        time_interval=60,
        linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT, 1)],
    ),
    RateLimit(
        USER_PORTFOLIO,
        limit=MAX_REQUEST,
        time_interval=60,
        linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT, 1)],
    ),
    RateLimit(
        USER_POSITIONS,
        limit=MAX_REQUEST,
        time_interval=60,
        linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT, 1)],
    ),
    RateLimit(
        USER_VAULTS,
        limit=MAX_REQUEST,
        time_interval=60,
        linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT, 1)],
    ),
    RateLimit(
        USER_WITHDRAW_TO_FUNDING,
        limit=MAX_REQUEST,
        time_interval=60,
        linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT, 1)],
    ),
]

CURRENCY = "WUSDC"
CURRENCY_DECIMALS = 10**6

# Used for the intent signature generation
SEED = "e2ac4e5688d964270ad876d760c2ebb2d54fb26d93512c790049b6583730d06f"  # noqa: mock
