import json

from redis.client import Redis


encoder = json.JSONEncoder()
decoder =json.JSONDecoder

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



class CryptofeedRedisCient(RedisClient):
    def __init__(self, host='localhost', port=6379, non_float_keys = ['exchange_name','symbol']):
        self.non_float_keys = non_float_keys
        super().__init__(host, port)

    def get(self, stream_name, last_n=1, return_all=False):
        tuples = self.redis_client.xrevrange(stream_name, max='+', min='-', count=last_n)
        if return_all:
            # Return tuples
            return tuples
        else:
            # Return a parsed list
            list_to_parse = [i[1] for i in tuples]
            if stream_name==L2_OB_REDIS_STREAM:
                parsed_list_to_return = self.parseList(list_to_parse)
            elif stream_name==TRADES_REDIS_STREAM:
                parsed_list_to_return = self.parseList(list_to_parse)

            if len(parsed_list_to_return)>0:
                return parsed_list_to_return[0] if last_n == 1 else parsed_list_to_return
            else:
                return None

    def parseList(self, list_to_parse):
        parsed_list_to_return = []
        for i in list_to_parse:
            dict_to_return = {}
            for key, value in i.items():
                if key.decode('utf-8') in self.non_float_keys:
                    dict_to_return[key.decode('utf-8')] = value.decode('utf-8').replace('"', "")
                else:
                    dict_to_return[key.decode('utf-8')] = float(value.decode('utf-8'))
            parsed_list_to_return.append(dict_to_return)
        return parsed_list_to_return