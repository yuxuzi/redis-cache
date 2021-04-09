from functools import wraps
import pickle
import redis
from datetime import timedelta
from typing import Callable, Optional, Any, Union
from abc import ABC, abstractmethod
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("cache decorator")


class Serializer(ABC):
    """
    Protocol for serializer, it is required to implement dumps to serialize and loads to deserialize
    """
    @abstractmethod
    def dumps(self, obj) -> Union[str, bytes]:
        pass
    @abstractmethod
    def loads(self, st: Union[str, bytes]) -> Any:
        pass


class Cache:
    """
    A simple Redis cache decorator

    """

    def __init__(self, client: redis.Redis, expire:Union[timedelta, int,None]=None, prefix: str = "rc",
                 serializer: Optional[Serializer] = None) -> None:
        """
        Args:
            client: Redis connection
            expire: Key expiry, could be set as timedelta explicitly or int, number of seconds
            prefix: The string to prefix the redis keys, by default "rc"
            serializer:The serializer to serial and deserialize function arguments and returns, it must implements
                       two method: dumps, loads
        """
        self.client = client

        # test Redis connection
        try:
            # getting None returns None or throws an exception
            client.ping()
            logger.info("Successfully connected to redis")
        except (redis.exceptions.ConnectionError,
                redis.exceptions.BusyLoadingError,
                ConnectionRefusedError):
            logger.error("Redis connection error!")
            self.connected = False
        else:
            self.connected = True

        self.prefix = prefix
        if serializer is None:
            self.serializer = pickle

        # set key expiry
        if expire is None:
            logger.warning("The default cache expiration set for 1 day!")
            self.expire = timedelta(days=1)
        else:
            self.expire = expire

        self.key_func = None

    def __call__(self, func: Callable) -> Callable:
        """
        Args:
            func: function to decorate

        """
        if not self.connected:
            logger.info("Compute result from function.")
            return func

        self.key_func = f'{self.prefix}:{func.__module__}.{func.__name__}'

        @wraps(func)
        def inner(*args, **kwargs):
            args_serialized = self.serializer.dumps([args, kwargs])
            key = f'{self.key_func}:{args_serialized}'
            result = self.client.get(key)
            if result:
                logger.info("Found and fetching result from cache.")
                return self.serializer.loads(result)
            result = func(*args, **kwargs)
            result_serialized = self.serializer.dumps(result)
            logger.info("Calculated result saved in cache.")
            self.client.setex(key, self.expire, result_serialized)
            return result

        # add invalidation callables
        inner.invalidate = self.invalidate
        inner.invalidate_all = self.invalidate_all
        return inner

    def invalidate(self, *args, **kwargs) -> None:
        """
        invalidate a cached result with args and kwargs
        """
        args_serialized = self.serializer.dumps([args, kwargs])
        key = f'{self.key_func}:{args_serialized}'
        self.client.delete(key)
        logger.info("Cached entry removed.")

    def invalidate_all(self, chunk_size: int = 100) -> None:
        """
        invalidate all cache for the function
        Args:
            chunk_size: chunk_size for Redis to delete together

        """
        keys = []
        for i, k in enumerate(self.client.scan_iter(f'{self.key_func}:*')):
            keys.append(k)
            if i % chunk_size == 0:
                self.client.delete(*keys)
        if keys:
            self.client.delete(*keys)
        logger.info("All values for cached function removed.")
