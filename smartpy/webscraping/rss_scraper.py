import random
import time

import html2text
import pandas as pd
import feedparser
import trafilatura

from smartpy.utility import dt_util
from smartpy.utility.log_util import getLogger

logger = getLogger(__name__)

converter = html2text.HTML2Text()
converter.ignore_links = True


class RSSScraper:
    def __init__(self, rss_feeds):
        self.rss_feeds = rss_feeds
        self.scrapers = [SingleFeedScraper(feed) for feed in rss_feeds]

    def listen(self):
        while True:
            for scraper in self.scrapers:
                data = scraper.scrape()
                if data is not None:
                    yield data


class SingleFeedScraper:
    def __init__(self, source):
        self.source, self.source_rss_feed_url = source
        self.entries_df = None

        self.last_update_timestamp = 0
        self.current_pull_frequency_seconds = 0

    def is_time_to_update(self):
        if time.time() - self.last_update_timestamp > self.current_pull_frequency_seconds:
            self.last_update_timestamp = time.time()
            logger.info(f"Starting {self.source} requests...")
            return True
        else:
            return False

    def fetch_feed(self):
        try:
            self.entries_df = feedparser.parse(self.source_rss_feed_url)['entries']
            if len(self.entries_df) == 0:
                logger.warning("No entries found in RSS feed")
            else:
                self.entries_df = pd.DataFrame(self.entries_df)
                self.entries_df['source'] = self.source
        except Exception as e:
            logger.info(f"Error fetching RSS feed: {e}")
            raise e


    def update_pull_frequency(self):
        self.entries_df['timestamp'] = self.entries_df['published'].apply(lambda x: dt_util.convertDatetimeTZ(dt_util.toDatetime(x), 'UTC'))
        publishing_frequency_seconds = self.entries_df['timestamp'].diff(-1).dt.total_seconds().ewm(alpha=0.5).mean().iloc[-1]
        self.current_pull_frequency_seconds = min(120, abs(publishing_frequency_seconds * 0.5))
        self.current_pull_frequency_seconds = self.current_pull_frequency_seconds * random.uniform(0.5, 1.0)

    def scrape(self):
        if not self.is_time_to_update():
            return None
        self.fetch_feed()
        if len(self.entries_df) > 0:
            self.update_pull_frequency()
            return self.entries_df
        else:
            logger.warning(f"No entries found in RSS feed for {self.source}")
            self.current_pull_frequency_seconds = 150
            return None
