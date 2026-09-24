import os

import redis
from dotenv import load_dotenv

load_dotenv()

DEFAULT_URL = os.getenv("REDIS_URL")
_client: redis.Redis | None = None


def init_redis(url: str | None = DEFAULT_URL) -> redis.Redis:
    """ Initialize the module-level Redis client. """
    # global _client
    # if url is None:
    #     raise RuntimeError("REDIS_URL is not set")
    # _client = redis.Redis.from_url(url, decode_responses=True)

    _client = redis.Redis(
        host=os.getenv('REDIS_URL'),
        port=os.getenv('REDIS_PORT'),
        decode_responses=True,
        username=os.getenv("REDIS_USERNAME"),
        password=os.getenv("REDIS_PASSWORD"),
    )
    return _client



def get_redis() -> redis.Redis:
    if _client is None:
        return init_redis()
    return _client
