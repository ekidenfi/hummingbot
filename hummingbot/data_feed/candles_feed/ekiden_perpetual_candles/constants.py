from bidict import bidict

from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit

REST_URL = "https://api.ekiden.fi"
WSS_URL = "wss://api.ekiden.fi/ws/public"
HEALTH_CHECK_ENDPOINT = "/ping"
CANDLES_ENDPOINT = "/api/v1/market/candles"
MARKET_ENDPOINT = "/api/v1/market/market_info"

INTERVALS = bidict(
    {
        "1m": 1,
        "5m": 5,
        "15m": 15,
        "1h": 60,
        "2h": 120,
        "4h": 240,
        "6h": 360,
        "12h": 720,
        "1d": "D",
        "3d": "3D",
        "1w": "W",
    }
)

MAX_RESULTS_PER_CANDLESTICK_REST_REQUEST = 1000
MAX_REQUEST = 600
ALL_ENDPOINTS_LIMIT = "All"

RATE_LIMITS = [
    RateLimit(ALL_ENDPOINTS_LIMIT, limit=MAX_REQUEST, time_interval=60),
    RateLimit(
        HEALTH_CHECK_ENDPOINT,
        limit=MAX_REQUEST,
        time_interval=60,
        linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT, 1)],
    ),
    RateLimit(
        CANDLES_ENDPOINT,
        limit=MAX_REQUEST,
        time_interval=60,
        linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT, 1)],
    ),
    RateLimit(
        MARKET_ENDPOINT,
        limit=MAX_REQUEST,
        time_interval=60,
        linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT, 1)],
    ),
]
