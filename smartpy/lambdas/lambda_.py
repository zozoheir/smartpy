import json
import re
import urllib
from abc import abstractmethod

from sqlalchemy import String, Column
from sqlalchemy.orm import declarative_base

from smartpy.data.postgres_db import PostgresDB


def camel_to_snake(name):
    return '_'.join(re.sub('([A-Z][a-z]+)', r' \1', name).split()).lower()


Base = declarative_base()

class Config(Base):
    __tablename__ = 'configs'
    __table_args__ = {'schema': 'configs'}

    namespace = Column(String)
    type = Column(String)
    key = Column(String, primary_key=True, nullable=False)
    value = Column(String)


class Lambda:

    def __init__(self,
                 redis_client,
                 postgres_db: PostgresDB,
                 namespace='',
                 expiry_seconds=60):
        self.redis_client = redis_client
        self.postgres_db = postgres_db
        self.namespace = 'lambdas:' + namespace
        self.key = camel_to_snake(self.__class__.__name__)
        self.expiry_seconds = expiry_seconds
        self.init_database()

    def init_database(self):
        if self.redis_client.get(f"{self.namespace}:init:{self.key}"):
            return

        with self.postgres_db.engine.begin() as conn:
            Base.metadata.create_all(conn)
        self.postgres_db.insert(table_name='configs.configs',
                                rows=[{
                                    'namespace': 'lambdas',
                                    'type': self.key,
                                    'key': f'is_on_{self.key}',
                                    'value': 'True'
                                }])
        self.redis_client.set(f"{self.namespace}:init:{self.key}", 'True')

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
