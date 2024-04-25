import json
import re
import urllib
from abc import abstractmethod


def camel_to_snake(name):
    return '_'.join(re.sub('([A-Z][a-z]+)', r' \1', name).split()).lower()


class Lambda:

    def __init__(self,
                 redis_client,
                 namespace='',
                 expiry_seconds=60):
        self.redis_client = redis_client
        self.namespace = 'lambdas:' + namespace
        self.key = camel_to_snake(self.__class__.__name__)
        self.expiry_seconds = expiry_seconds

    def get_redis_key(self,
                      **kwargs):
        encoded_params = urllib.parse.urlencode(kwargs)
        encoded_params = '?'+encoded_params if encoded_params else ''
        return f"{self.namespace}:{self.key}{encoded_params}"

    def get_cache(self, **kwargs):
        redis_key = self.get_redis_key(**kwargs)
        value = self.redis_client.get(redis_key)
        if value:
            return json.loads(value)

    def set_cache(self,
                  value,
                  **kwargs):
        redis_key = self.get_redis_key(**kwargs)
        return self.redis_client.set(redis_key, json.dumps(value), ex=self.expiry_seconds)

    @abstractmethod
    def run_lambda(self, **kwargs):
        pass

    @abstractmethod
    async def async_run_lambda(self, **kwargs):
        pass

    def run(self,
            **kwargs):
        cache = self.get_cache(**kwargs)
        if cache:
            return cache
        else:
            result = self.run_lambda(**kwargs)
            self.set_cache(result, **kwargs)
            return result

    async def async_run(self,
                        **kwargs):
        cache = self.get_cache(**kwargs)
        if cache:
            return cache
        else:
            result = await self.async_run_lambda(**kwargs)
            self.set_cache(result, **kwargs)
            return result
