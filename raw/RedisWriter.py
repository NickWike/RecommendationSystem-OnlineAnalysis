from redis import ConnectionPool as RedisPool
from redis import Redis


class BaseWriter(Redis):

    def __init__(self, redis_pool: RedisPool):
        super().__init__(connection_pool=redis_pool)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

