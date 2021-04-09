from redis_cache import Cache, SliceableCache
import pytest
import redis
import time
import numpy as np
import pandas as pd
import pickle
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("pytests")

client = redis.Redis()

@pytest.fixture(scope="session", autouse=True)
def clear_cache():
    client.flushall()


test_data1= [(2, 3, 5), ('A', 'B', 'AB'),
            ([1, 2], [3, 4], [1, 2, 3, 4])
            ]

@pytest.mark.parametrize("a,b,expected",test_data1)
def test_addition(a, b, expected):
    @Cache(client)
    def add(x, y):
        return x + y
    assert add(a, b) == expected

test_data2= [(2, 5, True, 40),
            (1, 10, False, 0.1)
            ]



@pytest.mark.parametrize("a,b,c, expected", test_data2)
def test_keyword(a, b,c,  expected):
    @Cache(client)
    def ratio(x, y, to_percent):
        if to_percent:
            return (x / y)*100
        return x/y

    assert ratio(a, b, to_percent=c) == expected
    assert ratio(a, b, to_percent=c) == expected



@pytest.mark.parametrize("a,b,expected",test_data1)
def test_expire(a, b, expected):
    @Cache(client, expire=1)
    def add(x, y):
        return x + y

    time.sleep(1)

    client.flushall()
    assert add(a,b)==expected

    args_serialized = pickle.dumps([a,b])
    key_func = f'rc:{add.__module__}.{add.__name__}'
    key = f'{key_func}:{args_serialized}'
    assert not client.get(key)


def test_sliceable():

    @SliceableCache(client)
    def slicedata(*, start, end):
        df = pd.DataFrame(np.random.randint(0, 100, size=(100, 4)), columns=list('ABCD'),
                          index=pd.date_range('2020-01-01', periods=100))
        logger.info("data frame created")

        return df.loc[start:end]

    slicedata(start='2020-01-01', end='2020-03-05')
    slicedata(start='2020-02-01', end='2020-02-20')
    slicedata(start='2020-02-01', end='2020-04-05')



