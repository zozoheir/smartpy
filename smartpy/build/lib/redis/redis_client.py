import json

from redis.client import Redis


encoder = json.JSONEncoder()
decoder = json.JSONDecoder

TRADES_REDIS_STREAM = 'trades'
L2_OB_REDIS_STREAM = 'order_book'

class RedisClient:

    def __init__(self, host='localhost', port=6379, verbose=False):
        self.redis_client = Redis(host, port)

    def get(self, stream_name, last_n=1, return_all=False):
        tuples = self.redis_client.xrevrange(stream_name, max='+', min='-', count=last_n)
        if return_all:
            return tuples
        else:
            if last_n==1:
                return tuples[0][1]
            else:
                return [i[1] for i in tuples]

    def add(self, stream_name, output_dict: dict):
        output_dict = {key: encoder.encode(value) for key, value in output_dict.items()}
        return self.redis_client.xadd(stream_name, output_dict)

    def parse_redis_keys(self, list_of_keys):
        return [i.decode('utf-8') for i in list_of_keys]

    def parse_redis_dicts(self, list_of_dicts):
        return [{k.decode('utf-8'): v.decode('utf-8') for k, v in i.items()} for i in list_of_dicts]

    def add_concurrently(self, stream_name, output_dict):
        self.add(stream_name, output_dict)

