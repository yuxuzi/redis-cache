from functools import wraps
import pandas as pd
from redis_cache.cache import Cache
from typing import Callable

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Sliceable Cache")


class SliceableCache(Cache):
    """
    a experiment on sliceable cache
    it applys to function that takes start, end as named range arguments, and returns
    a slice of a large datetime indexed series or dataframe, while the overlapped segment
    with different starts, ends are consistent including the index.
    """

    def __init__(self, *args, start_val: str = 'start', end_val: str = 'end', **kwargs) -> None:
        """

        Args:
            start_val: argument name of the start of the decorated function
            end_val: argument name of the end in of decorated function

        """
        super().__init__(*args, **kwargs)
        self.start_val = start_val
        self.end_val = end_val

    def __call__(self, func: Callable) -> Callable:

        if not self.connected:
            logger.info("Compute result from function.")
            return func

        self.key_func = f'{self.prefix}:{func.__module__}.{func.__name__}'

        @wraps(func)
        def inner(*args, **kwargs):
            kwargs0 = kwargs.copy()
            # pop the start, end arguments
            # the rest of the arguments are used as key
            start, end = kwargs.pop(self.start_val), kwargs.pop(self.end_val)
            args_serialized = self.serializer.dumps([args, kwargs])
            key = f'{self.key_func}:{args_serialized}'
            result = self.client.get(key)
            if result:
                start0, end0, data = self.serializer.loads(result)
                if isinstance(start0, pd.Timestamp):
                    start, end = pd.Timestamp(start), pd.Timestamp(end)
                # return slice of data if the range is available in cache
                if start0 <= start and end <= end0:
                    if not isinstance(data, (pd.DataFrame, pd.Series)):
                        logger.error("Time series or DataFrame are expected")
                        raise ValueError("Time series or DataFrame are expected")
                    logger.info("Found and fetching data from cache")
                    return data.loc[start: end]

            data = func(*args, **kwargs0)
            if not isinstance(data, (pd.DataFrame, pd.Series)):
                logger.error("Series or DataFrame are expected")
                raise ValueError("Series or DataFrame are expected")
            logger.info("Calculated result is saved in cache")
            start0, end0 = data.index[[0, -1]]
            result_serialized = self.serializer.dumps([start0, end0, data])
            self.client.setex(key, self.expire, result_serialized)

        inner.invalidate = self.invalidate
        inner.invalidate_all = self.invalidate_all
        return inner
