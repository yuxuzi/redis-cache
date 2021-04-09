redis-cache
===========
A simple Redis cache implementation for python functions.  
It exports two decorators,  a generic Cache, and a special SliceableCache for sequence data.

-  The decorator support multi-user/multi-machine invocations for users executing the same function calls.
on different computers.

- The decorator caches the function signature and  result into  Redis key value database.

- The decorator allow user to set up cache-expiry.

- The SliceableChace support function that take start, end range parameters and return a slice dataframe. Assume that
  the range parameters make the function a slice of a large invariable otherwise dataframe.

- The arguments and the return types must be serializable,by default pickle. User may choose other serializer as long as it provide dumps to serialize and loads to deserialize.

- By default, the Redis evicts the least recently used keys out of all keys with an expire field set, according to volatile-lru policy. User may choose other policy through configuration.

How to use it
===========
```
from redis_cache import CacheDecorator, SliceableCache

# make sure the Rdis server is up running.
#setup connection
client = redis.Redis()

# decorate the function to cache
@Cache(
def foo(arg1, arg2,*, karg1, karg1):
    ...
    return result
    
# call the function
foo( 2, 3, bar=5)

# invalidate a cached entry
foo.invalidate(2,3)



# invalidate all cache for the function
foo.invalidate_all



# sliceable cache
@SliceableCache(client)
def slicedata(*, start, end):
    df = pd.DataFrame(np.random.randint(0, 100, size=(100, 4)), columns=list('ABCD'),
                      index=pd.date_range('2020-01-01', periods=100))
    logger.info("data frame created")

    return df.loc[start:end]


df = slicedata(start='2020-02-09', end='2020-03-05')

print(df

```
