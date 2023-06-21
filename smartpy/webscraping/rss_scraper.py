import random

import time
import smartpy.utility.data_util as data_util
import feedparser
import pandas as pd
from smartpy.aws.s3 import S3
import datetime as dt

from smartpy.utility import dt_util

REQUIRED_COLS = ['timestamp', 'AUTHOR', 'title', 'link', 'content_image_url', 'thumbnail_image_url', 'date',
                 'signal_source', 'id']

REQUIRED_RAW_COLUMNS = ['published','updated','influencers','AUTHOR','title','summary','description','content','link','id','media_content','media_thumbnail']


class RSSScraper:

    def __init__(self,
                 source_name,
                 source_rss_feed_url,
                 s3_bucket,
                 s3_dir,
                 table_name):
        self.source_name = source_name
        self.source_rss_feed_url = source_rss_feed_url
        self.s3_bucket = s3_bucket
        self.s3_dir = s3_dir
        self.table_name = table_name
        self.s3_file_uri = f"s3://{self.s3_bucket}/{self.s3_dir}/{table_name}_raw.parquet"

        self.entries_df = None
        self.raw_df = None
        self.processed_df = None

        # Internal variables for parsing
        self.last_update_timestamp = 0
        self.current_pull_frequency_seconds = 0

    def print(self, msg):
        print(str(dt.datetime.now()).split('.')[0]+' : '+msg)

    def isTimeToUpdate(self):
        if time.time() - self.last_update_timestamp > self.current_pull_frequency_seconds:
            self.last_update_timestamp = time.time()
            self.print(f"Starting {self.source_name} requests...")
            return True
        else:
            return False

    def scrape(self):
        # Parse entries into dataframe
        self.entries_df = feedparser.getChunks(self.source_rss_feed_url)['entries']
        self.entries_df = pd.DataFrame(self.entries_df)
        # published_parsed cause errors when uploading to parquet
        if 'published_parsed' in self.entries_df.columns:
            self.entries_df = self.entries_df.drop('published_parsed', axis=1)

        # Create raw open_orders to have similar columns across all rss feeds
        self.raw_df = pd.DataFrame()
        for col in REQUIRED_RAW_COLUMNS:
            if col in self.entries_df.columns:
                self.raw_df[col] = self.entries_df[col]
            else:
                self.raw_df[col] = float('nan')

    def addIDColumns(self):
        self.raw_df['source_name'] = self.source_name
        self.raw_df['xid'] = self.source_name

        # Each RSS feed gives timestamp as either 'published' or 'updated'
        if 'published' in self.entries_df.columns:
            self.raw_df['xid'] = self.raw_df['xid'] + '-' + self.raw_df['published'].apply(lambda x: str(pd.to_datetime(x).timestamp()))
        elif 'updated' in self.entries_df.columns:
            self.raw_df['xid'] = self.raw_df['xid'] + '-' + self.raw_df['updated'].apply(
                lambda x: str(pd.to_datetime(x).timestamp()))

    def updatePullFrequency(self):
        """
        We check the RSS feed at a dynamic frequency based on recent frequency of publication
        """
        try:
            ewm_publishing_frequency_seconds = self.raw_df['published'].apply(pd.to_datetime).diff(1).dt.total_seconds().ewm(alpha=0.5).mean()[
                len(self.raw_df) - 1]
        except TypeError:
            # We do this in the case that given timestamps do not have proper timezones
            self.raw_df['published'] = self.raw_df['published'].apply(lambda x:dt_util.convertDatetimeTZ(dt_util.toDatetime(x), 'UTC'))
            ewm_publishing_frequency_seconds = self.raw_df['published'].diff(1).dt.total_seconds().ewm(alpha=0.5).mean()[
                len(self.raw_df) - 1]


        self.current_pull_frequency_seconds = min(60*60,abs(ewm_publishing_frequency_seconds * 0.5))
        self.current_pull_frequency_seconds = self.current_pull_frequency_seconds * random.randint(5,10)/10
