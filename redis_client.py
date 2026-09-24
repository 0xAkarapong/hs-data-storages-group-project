import os

import redis
from dotenv import load_dotenv

load_dotenv()

DEFAULT_URL = os.getenv("REDIS_URL")
_client: redis.Redis | None = None


def init_redis(url: str | None = DEFAULT_URL) -> redis.Redis:
    """ Initialize the module-level Redis client. """
    global _client
    if not url:
        raise RuntimeError("REDIS_URL is not set")
    if "://" in url:
        _client = redis.Redis.from_url(url, decode_responses=True)
    else:
        _client = redis.Redis(
            host=url,
            port=int(os.getenv("REDIS_PORT") or 6379),
            username=os.getenv("REDIS_USER") or os.getenv("REDIS_USERNAME") or None,
            password=os.getenv("REDIS_PASSWORD") or None,
            decode_responses=True,
        )
    return _client



def get_redis() -> redis.Redis:
    if _client is None:
        return init_redis()
    return _client
