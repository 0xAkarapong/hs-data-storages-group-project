import os

import redis
from dotenv import load_dotenv

load_dotenv()

DEFAULT_URL = os.getenv("REDIS_URL")
# The shared class Redis allows 30 clients in total (maxclients), for everyone.
# A blocking pool makes extra threads wait for a free connection instead of
# opening a new one per thread and getting "max number of clients reached".
MAX_CONNECTIONS = int(os.getenv("REDIS_MAX_CONNECTIONS") or 10)
POOL_TIMEOUT_SECONDS = 30
_client: redis.Redis | None = None


def init_redis(url: str | None = DEFAULT_URL) -> redis.Redis:
    """ Initialize the module-level Redis client. """
    global _client
    if not url:
        raise RuntimeError("REDIS_URL is not set")
    pool_options = {"max_connections": MAX_CONNECTIONS, "timeout": POOL_TIMEOUT_SECONDS, "decode_responses": True}
    if "://" in url:
        pool = redis.BlockingConnectionPool.from_url(url, **pool_options)
    else:
        pool = redis.BlockingConnectionPool(
            host=url,
            port=int(os.getenv("REDIS_PORT") or 6379),
            username=os.getenv("REDIS_USER") or os.getenv("REDIS_USERNAME") or None,
            password=os.getenv("REDIS_PASSWORD") or None,
            **pool_options,
        )
    _client = redis.Redis(connection_pool=pool)
    return _client



def get_redis() -> redis.Redis:
    if _client is None:
        return init_redis()
    return _client
