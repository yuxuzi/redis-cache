## Context
Implement a python module (or optionally, a package) that exports a decorator of
some kind (`cache`) and any other necessary utilities. The ultimate goal of the
decorator is to augment functions such that their invocations are *persistent
and results do not need to be re-computed*. Your decorator should support
multi-user/multi-machine invocations for users executing the same function calls
on different computers! You can do this in any way you see fit - for example
storing pickled function results in a key-value store keyed by a unique
identifier for each invocation of the functions - or something more robust. Your
decorator should have an api similar to the following:

```
@cache(...)
def foo(...):
    ...
    return ...

# users with the same source code as the above should be able to take advantage
# of other users' function invocations. If Alice fetches foo(1, 2, bar="bar")
# then Bob should not need to recompute foo(1, 2, bar="bar")!
```

test_redis_cache.py

## Requirements

- your decorator needs to be able to handle multiple python functions and should not
confuse invocation results.

- your decorator should store things in a setting that is relatively
high-performance (s3, for example, may not be a good candidate).

- you can assume the cached function(s) are (mostly) referentially-transparent;
however, in the real world, things change. Therefore, it may be helpful to
provide options for cache-expiry.

- What about some common, adversarial use cases such as functions that take date
ranges and return a slice of a pandas dataframe reflecting that date. How could
your decorator be expanded to fetch only necessary information? You do not have
to implement this, but your implementation should be ready-for-change for a
feature like this.

Please complete this task at your earliest convenience to a level that you see
fit - you can send us your implementation anytime next week. If you have any
questions about the prompt or the motivation behind it, donâ€™t hesitate to reach
out. We hope you are well and look forward to speaking with you again.