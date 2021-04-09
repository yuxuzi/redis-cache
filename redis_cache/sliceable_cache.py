from functools import wraps
import pandas as pd
from redis_cache.cache import Cache
from typing import Callable, Optional, Any, Union

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Sliceable Cache")


class SliceableCache(Cache):
    """
    a experiment on sliceable cache
    it apply to function that takes start, end as named range arguments, and returns
    a slice of a large otherwise invariable datetime ,or series indexed dataframe, i.e.,
    the overlapped segment of the returned dataframes or series with difference
     starts, ends stay the same including the index.
    """

    def __init__(self, *args, start_val: str = 'start', end_val: str = 'end', **kwargs) -> None:
        """

        Args:
            start_val: argument name of the start in the decorated function
            end_val: argument name of the end in the decorated function

        """
        super().__init__(*args, **kwargs)
        self.start_val = start_val
        self.end_val = end_val

    def __call__(self, func):

        if not self.connected:
            logger.info("Compute result from function.")
            return func

        self.key_func = f'{self.prefix}:{func.__module__}.{func.__name__}'

        @wraps(func)
        def inner(*args, **kwargs):
            kwargs0 = kwargs.copy()
            # pop the start, end arguments
            # the rest of the arguments are used as keys
            start, end = kwargs.pop(self.start_val), kwargs.pop(self.end_val)
            args_serialized = self.serializer.dumps([args, kwargs])
            key = f'{self.key_func}:{args_serialized}'
            result = self.client.get(key)
            if result:
                start0, end0, data = self.serializer.loads(result)
                if isinstance(start0, pd.Timestamp):
                    start, end = pd.Timestamp(start), pd.Timestamp(end)
                # return slice if range is avaliable in cache
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


if __name__ == "__main__":
    import redis
    import numpy as np
    import pandas as pd

    client = redis.Redis()

    np.random.seed(0)


    @SliceableCache(client)
    def slicedata(*, start, end):
        df = pd.DataFrame(np.random.randint(0, 100, size=(100, 4)), columns=list('ABCD'),
                          index=pd.date_range('2020-01-01', periods=100))
        logger.info("data frame created")

        return df.loc[start:end]


    df = slicedata(start='2020-02-09', end='2020-03-05')

    logger.info(df.head())

    df = slicedata(start='2020-02-20', end='2020-03-05')
    logger.info(df.head())
