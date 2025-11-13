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

ORDER_STATUSES = {
    "open": OrderState.OPEN,
    "resting": OrderState.OPEN,
    "filled": OrderState.FILLED,
    "canceled": OrderState.CANCELED,
    "rejected": OrderState.FAILED,
    "reduceOnlyCanceled": OrderState.CANCELED,
    "perpMarginRejected": OrderState.FAILED,
}

WS_TRADES_TOPIC = "trade"
WS_ORDERBOOK_TOPIC = "orderbook"
PUBLIC_TOPICS = [WS_TRADES_TOPIC, WS_ORDERBOOK_TOPIC]

WS_USER_ORDER_TOPIC = "order"
WS_USER_POSITION_TOPIC = "position"
WS_USER_FILL_TOPIC = "fill"
PRIVATE_TOPICS = [WS_USER_ORDER_TOPIC, WS_USER_POSITION_TOPIC, WS_USER_FILL_TOPIC]

ORDER_NOT_EXIST_MESSAGE = "order"
UNKNOWN_ORDER_MESSAGE = "Order was never placed, already canceled, or filled"

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

CURRENCY = "USD"
CURRENCY_DECIMALS = 10**6
