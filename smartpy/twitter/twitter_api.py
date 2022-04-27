import datetime as dt
import logging
import random
from typing import Union

import pandas as pd
import pytz
import snscrape.modules.twitter as sntwitter
import twitter

import smartpy.nlp.text as nlp_text

logging.getLogger("snscrape").setLevel(logging.WARNING)

# ENDPOINTS
USERTIMELINE_ENDPOINT_URL = 'https://api.twitter.com/1.1/statuses/user_timeline.json'
TWITTER_SEARCH_NON_NATIVE_FIELDS = ['count']


class Twitter:


    def _raw_search(self, parameters):
        """
        :param parameters:
        :param count:
        :return:
        """
        # since, count or within_time fields are needed to stop the search at some point
        assert sum([i in ['since','count','within_time'] for i in parameters.keys()])>0, "You need a searchlookback cutoff"
        if 'count' in parameters.keys():
            count = parameters['count']
        else:
            count = None
        search_quote = self._params_to_search_quote(parameters)
        tweets_list = []
        for i, tweet in enumerate(sntwitter.TwitterSearchScraper(search_quote).get_items()):
            if count and i >= count:
                break
            tweets_list.append([tweet.date, tweet.id, tweet.username, tweet.content, dir(tweet)])

        return pd.DataFrame(tweets_list, columns=['timestamp', 'id', 'author', 'text','meta'])


    def search(self, parameters):

        # we need searches one by one otherwise it doesn't return correct results
        if 'from' in parameters.keys():
            if isinstance(parameters['from'], str):
                parameters['from'] = [parameters['from']]
            df = pd.DataFrame()
            for handle in parameters['from']:
                tmp_params = parameters.copy()
                tmp_params['from'] = handle
                df = df.append(self._raw_search(tmp_params))
            return df
        else:
            return self._raw_search(parameters)


    def _params_to_search_quote(self, search_parameters):
        parameters = search_parameters.copy()
        search_quote = ""
        # content field doesn't exist so we remove it and put string to search at beginning of search quote
        if "content" in parameters.keys():
            search_quote = " OR ".join(parameters['content'])
        for key, value in parameters.items():
            if key not in TWITTER_SEARCH_NON_NATIVE_FIELDS and key in ['from','within_time','since','until','min_retweets']:
                if isinstance(value, list):
                    value = " OR ".join(value)
                    search_quote = f"{key}:{value}"
                else:
                    search_quote = search_quote + f" {key}:{value}"
        return search_quote

