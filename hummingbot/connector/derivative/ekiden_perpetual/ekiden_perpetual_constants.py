import re
from enum import Enum

from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit
from hummingbot.core.data_type.in_flight_order import OrderState

EXCHANGE_NAME = "ekiden_perpetual"
DOMAIN = ""
BROKER_ID = "HBOT"

API_VERSION = "/api/v1"
PERPETUAL_BASE_URL = "https://api.dev.ekiden.fi"
PERPETUAL_WS_URL = "wss://api.dev.ekiden.fi/ws"

HEALTH_URL = "/info"

MARKET_INFO = "/market/tickers"
MARKET_STATS = "/market/tickers"
MARKET_FUNDING = "/market/funding/history"

AUTH_URL = "/authorize"

ORDER_PLACE = "/order/place"
ORDER_CANCEL = "/order/cancel"
ORDER_REALTIME = "/order/realtime"
ORDER_HISTORY = "/order/history"
EXECUTION_LIST = "/execution/list"

POSITION_LIST = "/position/list"
POSITION_SET_LEVERAGE = "/position/set-leverage"

ACCOUNT_BALANCE = "/account/balance"

WS_PUBLIC = "/public"
WS_PRIVATE = "/private"

WS_TRADES = "trade"
WS_ORDERBOOK = "orderbook"
WS_TICKER = "ticker"
PUBLIC_TOPICS = [WS_TRADES, WS_ORDERBOOK, WS_TICKER]

WS_USER_ORDER = "order"
WS_USER_POSITION = "position"
WS_USER_FILL = "execution"
PRIVATE_TOPICS = [WS_USER_ORDER, WS_USER_POSITION, WS_USER_FILL]


class TimeInForce(Enum):
    GTC = "GTC"
    IOC = "IOC"
    FOK = "FOK"
    POST_ONLY = "PostOnly"


class OrderSide(Enum):
    BUY = "Buy"
    SELL = "Sell"


class OrderTypeString(Enum):
    LIMIT = "Limit"
    MARKET = "Market"


class MarginMode(Enum):
    ISOLATED = "Isolated"
    CROSS = "Cross"


class TpSlMode(Enum):
    FULL = "FULL"
    PARTIAL = "PARTIAL"


class TpSlOrderType(Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"


ORDER_STATUSES = {
    "New": OrderState.OPEN,
    "PartiallyFilled": OrderState.PARTIALLY_FILLED,
    "Filled": OrderState.FILLED,
    "CancelRequested": OrderState.PENDING_CANCEL,
    "Canceled": OrderState.CANCELED,
    "Rejected": OrderState.FAILED,
    "PartiallyFilledAndCancelled": OrderState.PARTIALLY_FILLED,
}

AUTH_ERROR = {"code": "UNAUTHORIZED", "message": "Unauthorized"}
ORDER_NOT_ACTIVE = {
    "code": "BAD_REQUEST",
    "message": "Bad request: Order {sid} status is not active, cannot be cancelled",
}
ORDER_NOT_FOUND = {"code": "NOT_FOUND", "message": "Not found: Order {sid} not found"}

SID_REGEX = re.compile(r"\b[a-f0-9]{64}\b")
HEARTBEAT_TIME_INTERVAL = 20.0
FUNDING_INTERVAL_SECONDS = 3600

CURRENCY = "USDC"
QUOTE_DECIMALS = 6

MAX_REQUEST = 600
ALL_ENDPOINTS_LIMIT = "All"

RATE_LIMITS = [
    RateLimit(ALL_ENDPOINTS_LIMIT, limit=MAX_REQUEST, time_interval=60),
    RateLimit(
        HEALTH_URL,
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
        MARKET_STATS,
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
        AUTH_URL,
        limit=MAX_REQUEST,
        time_interval=60,
        linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT, 1)],
    ),
    RateLimit(
        ORDER_PLACE,
        limit=MAX_REQUEST,
        time_interval=60,
        linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT, 1)],
    ),
    RateLimit(
        ORDER_CANCEL,
        limit=MAX_REQUEST,
        time_interval=60,
        linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT, 1)],
    ),
    RateLimit(
        ORDER_REALTIME,
        limit=MAX_REQUEST,
        time_interval=60,
        linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT, 1)],
    ),
    RateLimit(
        ORDER_HISTORY,
        limit=MAX_REQUEST,
        time_interval=60,
        linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT, 1)],
    ),
    RateLimit(
        EXECUTION_LIST,
        limit=MAX_REQUEST,
        time_interval=60,
        linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT, 1)],
    ),
    RateLimit(
        POSITION_LIST,
        limit=MAX_REQUEST,
        time_interval=60,
        linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT, 1)],
    ),
    RateLimit(
        POSITION_SET_LEVERAGE,
        limit=MAX_REQUEST,
        time_interval=60,
        linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT, 1)],
    ),
    RateLimit(
        ACCOUNT_BALANCE,
        limit=MAX_REQUEST,
        time_interval=60,
        linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT, 1)],
    ),
]
