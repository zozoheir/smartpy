import os

import pandas as pd
import smartpy.redis.redis_client as redis
from smartpy.aws.s3 import S3
import datetime as dt
import smartpy.utility.data_util as data_util
import time
from smartpy.constants import *
import logging

logging.basicConfig(level=logging.INFO)


s3 = S3()

boto3_session = boto3.session.Session()
s3 = S3()
datautil = data_util.DataUtil()
cryptofeed_redis_client = redis.CryptofeedRedisCient()
getTime = lambda : str(dt.datetime.now()).split('.')[0]

class RedisStreamBatch:

    def __init__(self,
                 stream_name,
                 batch_size,
                 batch_processing_frequency_min,
                 live_buffer_size,
                 s3_bucket,
                 s3_file_key):

        self.stream_name = stream_name
        self.batch_processing_frequency_min = batch_processing_frequency_min
        self.batch_size = batch_size

        self.live_buffer_size = live_buffer_size
        self.s3_bucket = s3_bucket
        self.s3_file_key = s3_file_key

        self.last_batch_processing_throttle_timestamp = time.time()

        self.redis_batch_tuples = []
        self.batch_keys = []
        self.keys_to_remove = []

    def loadBatch(self):
        current_redis_stream_length = cryptofeed_redis_client.redis_client.xlen(self.stream_name)
        if time.time() - self.last_batch_processing_throttle_timestamp >= 60 * self.batch_processing_frequency_min \
                or current_redis_stream_length > self.batch_size + self.live_buffer_size:
            logging.info(f"-------------------------")
            logging.info(f"{getTime()} : {str(dt.datetime.now()).split('.')[0]} : Processing stream  -{self.stream_name}-")
            # Set throttle timestamps
            self.last_batch_processing_throttle_timestamp = time.time()
            # Query batch from redis, parse and upload to S3
            # We query 5 times the normal batch size if the queue has accumulated > batch_size to avoid bottlenecks
            if current_redis_stream_length > self.batch_size:
                batch_size_to_use = self.batch_size * 5
            else:
                batch_size_to_use = self.batch_size
            self.redis_batch_tuples = cryptofeed_redis_client.get(self.stream_name, last_n=batch_size_to_use,
                                                                  return_all=True)
            logging.info(f"{getTime()} : {self.stream_name} stream size before processing: {current_redis_stream_length}")
        else:
            self.redis_batch_tuples = []
            self.batch_keys = []
            self.keys_to_remove = []

    def pushToS3(self):
        if len(self.redis_batch_tuples) > 0:
            # Upload batch and delete keys from stream except live_pnd buffer
            self.batch_keys = [i[0] for i in self.redis_batch_tuples]
            self.keys_to_remove = self.batch_keys[self.live_buffer_size:]
            self.uploadRedisDictsList(self.redis_batch_tuples)

    def purge(self):
        if len(self.keys_to_remove) > 0:
            logging.info(f'{getTime()} : Purging stream')
            cryptofeed_redis_client.redis_client.xdel(self.stream_name, *self.keys_to_remove)
            current_redis_stream_length = cryptofeed_redis_client.redis_client.xlen(self.stream_name)
            logging.info(f"{getTime()} : Stream size after purging: {current_redis_stream_length}")

    # Each stream has a way of parsing its dicts.
    # Each class will have either the standard way from pm_redis_client, or will
    # overwrite with a specific parsing function like snapshots
    def parseRedisStreamDicts(self, redis_dicts_list):
        return cryptofeed_redis_client.parseList(redis_dicts_list)

    def uploadRedisDictsList(self, raw_redis_list_of_tuples):
        batch_keys = [i[0] for i in raw_redis_list_of_tuples]
        batch_dicts = [i[1] for i in raw_redis_list_of_tuples]
        parsed_redis_batch_dicts = self.parseRedisStreamDicts(batch_dicts)
        # Upload batch
        current_date = str(dt.datetime.now()).split(' ')[0]
        new_file_s3_uri = f's3://{self.s3_bucket}/{self.s3_file_key}'
        df = pd.DataFrame(parsed_redis_batch_dicts)
        df['redis_key'] = batch_keys
        df['date'] = current_date
        logging.info(f'Uploading {len(df)} {self.stream_name} rows to S3')
        datautil.toS3ParquetDataset(df, new_file_s3_uri, partition_cols=['date'])


class CryptoFeedTradesStreamBatch(RedisStreamBatch):

    def __init__(self,
                 batch_size,
                 batch_processing_frequency_min,
                 live_buffer_size=0,
                 s3_bucket=None,
                 s3_file_key=f"market_data/{os.environ['CRYPTOFEED_GROUP']}/cryptofeed/trades.parquet"):
        super().__init__(stream_name=redis.TRADES_REDIS_STREAM,
                         batch_size=batch_size,
                         batch_processing_frequency_min=batch_processing_frequency_min,
                         live_buffer_size=live_buffer_size,
                         s3_bucket=s3_bucket,
                         s3_file_key=s3_file_key,
                         )
    def uploadRedisDictsList(self, raw_redis_list_of_tuples):
        batch_keys = [i[0] for i in raw_redis_list_of_tuples]
        batch_dicts = [i[1] for i in raw_redis_list_of_tuples]
        parsed_redis_batch_dicts = self.parseRedisStreamDicts(batch_dicts)
        # Upload batch
        current_date = str(dt.datetime.now()).split(' ')[0]
        new_file_s3_uri = f's3://{self.s3_bucket}/{self.s3_file_key}'
        df = pd.DataFrame(parsed_redis_batch_dicts)
        df['redis_key'] = batch_keys
        df['date'] = current_date
        logging.info(f'{getTime()} : Uploading {len(df)} {self.stream_name} rows to S3')
        datautil.toS3ParquetDataset(df, new_file_s3_uri, partition_cols=['date'])


class CryptoFeedOBStreamBatch(RedisStreamBatch):

    def __init__(self,
                 batch_size,
                 batch_processing_frequency_min,
                 live_buffer_size=0,
                 s3_bucket=None,
                 s3_file_key=f"market_data/{os.environ['CRYPTOFEED_GROUP']}/cryptofeed/l2_orderbook.parquet"):
        super().__init__(stream_name=redis.L2_OB_REDIS_STREAM,
                         batch_size=batch_size,
                         batch_processing_frequency_min=batch_processing_frequency_min,
                         live_buffer_size=live_buffer_size,
                         s3_bucket=s3_bucket,
                         s3_file_key=s3_file_key,
                         )
    def uploadRedisDictsList(self, raw_redis_list_of_tuples):
        batch_keys = [i[0] for i in raw_redis_list_of_tuples]
        batch_dicts = [i[1] for i in raw_redis_list_of_tuples]
        parsed_redis_batch_dicts = self.parseRedisStreamDicts(batch_dicts)
        # Upload batch
        current_date = str(dt.datetime.now()).split(' ')[0]
        new_file_s3_uri = f's3://{self.s3_bucket}/{self.s3_file_key}'
        df = pd.DataFrame(parsed_redis_batch_dicts)
        df['redis_key'] = batch_keys
        df['date'] = current_date
        logging.info(f'{getTime()} : Uploading {len(df)} {self.stream_name} rows to S3')
        datautil.toS3ParquetDataset(df, new_file_s3_uri, partition_cols=['date'])

