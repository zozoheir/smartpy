import logging
import tweepy

import pandas as pd
import snscrape.modules.twitter as sntwitter

logging.getLogger("snscrape").setLevel(logging.WARNING)

# ENDPOINTS
USERTIMELINE_ENDPOINT_URL = 'https://api.twitter.com/1.1/statuses/user_timeline.json'
TWITTER_SEARCH_NON_NATIVE_FIELDS = ['count']


TWITTER_API_KEYS = {
    "email_address": "zozoheir.trading@gmail.com",
    "username": "TradingBothmane",
    "consumer_key": "28R3Kp8rlqNvTEvH837cKtLo4",
    "consumer_secret": "Ha1R1Jl5T7P8TVQFXixmel02q9zmdmvK5rpAh5utqnK51FT44f",
    "access_token_key": "1360664710664847363-7KnKk45zPbGJL5sYwzbfZP4zTeZMDa",
    "access_token_secret": "b37uluUYwzDY0HbUFzpez0spEsMxfwBJHZ9LJK1pr4toK"
}



class Twitter:

    def __init__(self):
        # assign the values accordingly
        consumer_key = "28R3Kp8rlqNvTEvH837cKtLo4"
        consumer_secret = "Ha1R1Jl5T7P8TVQFXixmel02q9zmdmvK5rpAh5utqnK51FT44f"
        access_token = "1360664710664847363-7KnKk45zPbGJL5sYwzbfZP4zTeZMDa"
        access_token_secret = "b37uluUYwzDY0HbUFzpez0spEsMxfwBJHZ9LJK1pr4toK"

        # authorization of consumer key and consumer secret
        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)

        # set access to user's access key and access secret
        auth.set_access_token(access_token, access_token_secret)
        self.api = tweepy.API(auth)

    def _raw_search(self, parameters):
        """
        :param parameters:
        :param count:
        :return:
        """
        # since, count or within_time fields are needed to stop the search at some point
        assert sum([i in ['since','count','within_time'] for i in parameters.keys()])>0, "You need a search lookback cutoff"
        if 'count' in parameters.keys():
            count = parameters['count']
        else:
            count = None
        search_quote = self._params_to_search_quote(parameters)

        tweets_list = []
        for i, tweet in enumerate(sntwitter.TwitterSearchScraper(search_quote).get_items()):
            if count and i >= count:
                break
            tweets_list.append(tweet)
        return tweets_list


    def search(self, parameters):

        # we need searches one by one otherwise it doesn't return correct results
        if 'from' in parameters.keys():
            if isinstance(parameters['from'], str):
                parameters['from'] = [parameters['from']]
            tweets_list = []
            for handle in parameters['from']:
                tmp_params = parameters.copy()
                tmp_params['from'] = handle
                tweets_list += self._raw_search(tmp_params)
            return tweets_list
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
