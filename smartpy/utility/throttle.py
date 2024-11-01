import datetime
from datetime import datetime, timedelta
from functools import wraps
import asyncio
import inspect

class throttle:
    """
    Decorator that prevents a function from being called more than once every
    time period.
    To create a function that cannot be called more than once a minute:
        @throttle(minutes=1)
        async def my_fun():
            pass
    """
    def __init__(self, seconds=0, minutes=0, hours=0):
        self.throttle_period = timedelta(
            seconds=seconds, minutes=minutes, hours=hours
        )
        self.time_of_last_call = datetime.min

    def __call__(self, fn):
        if inspect.iscoroutinefunction(fn):
            # Handle async function
            @wraps(fn)
            async def async_wrapper(*args, **kwargs):
                now = datetime.now()
                time_since_last_call = now - self.time_of_last_call

                if time_since_last_call > self.throttle_period:
                    self.time_of_last_call = now
                    return await fn(*args, **kwargs)
            return async_wrapper
        else:
            # Handle regular function
            @wraps(fn)
            def sync_wrapper(*args, **kwargs):
                now = datetime.now()
                time_since_last_call = now - self.time_of_last_call

                if time_since_last_call > self.throttle_period:
                    self.time_of_last_call = now
                    return fn(*args, **kwargs)
            return sync_wrapper
