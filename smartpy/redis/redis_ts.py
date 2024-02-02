import redis

class RedisTS:
    def __init__(self, host='localhost', port=6379):
        self.r = redis.Redis(host=host, port=port, decode_responses=True)
        self.ts = self.r.ts()

    def create_series(self, key, **kwargs):
        return self.ts.create(key, **kwargs)

    def add_sample(self, key, timestamp, value, **kwargs):
        return self.ts.add(key, timestamp, value, **kwargs)

    def get_range(self, key, from_time, to_time):
        return self.ts.range(key, from_time, to_time)
