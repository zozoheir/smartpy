import datetime as dt

from jsonpath_ng import parse

from twitter.scraper import Scraper
from twitter.search import Search
from twitter.account import Account
from twscrape import AccountsPool, API


class TwitterGraph:

    def __init__(self,
                 username,
                 password,
                 email,
                 ):

        self.search = Search(username, password, email, save=False)
        self.scraper = Scraper(username, password, email,  save=False)
        self.account = Account(username, password, email, save=False)


    def get_timeline(self,
                     limit=200):
        timeline = self.account.home_latest_timeline(limit=limit)
        entries = timeline[0]['data']['home']['home_timeline_urt']['instructions'][0]['entries']
        tweet_dicts = [self.extract_tweet_info(e) for e in entries]
        tweet_dicts = [t for t in tweet_dicts if t is not None]
        return tweet_dicts

    def get_user_stats(self, user_dict):
        # Compile the jsonpath expressions

        likes_expr = parse("$..legacy.favourites_count")
        tweets_and_replies_expr = parse("$..legacy.statuses_count")
        following_expr = parse("$..legacy.friends_count")
        followers_expr = parse("$..legacy.followers_count")
        account_creation_expr = parse("$..legacy.created_at")

        try:
            # Use the expressions to find the values
            user_id = user_dict['data']['user']['result']['rest_id']
            likes = [match.last_value for match in likes_expr.find(user_dict)][0]
            tweets_and_replies = [match.last_value for match in tweets_and_replies_expr.find(user_dict)][0]
            following = [match.last_value for match in following_expr.find(user_dict)][0]
            followers = [match.last_value for match in followers_expr.find(user_dict)][0]
            account_creation_str = [match.last_value for match in account_creation_expr.find(user_dict)][0]

            # Convert the timestamp to a datetime object
            account_creation = dt.datetime.strptime(account_creation_str, '%a %b %d %H:%M:%S %z %Y')

            return {
                "user_id": user_id,
                "likes": likes,
                "tweets_and_replies": tweets_and_replies,
                "following": following,
                "followers": followers,
                "account_creation": account_creation
            }

        except Exception as e:
            print(f"Failed to extract user stats: {e}")
            return None

    def extract_tweet_info(self, tweet_dict):
        # Compile the jsonpath expressions
        tweet_id_expr = parse('$..rest_id')
        screen_name_expr = parse("$..['user_results'].result.legacy.screen_name")
        followers_count_expr = parse("$..['user_results'].result.legacy.followers_count")
        url_expr = parse("$..['user_results'].result.legacy.screen_name")
        text_expr = parse("$..legacy.full_text")
        timestamp_expr = parse("$..legacy.created_at")
        image_url_expr = parse("$..['user_results'].result.legacy.profile_image_url_https")
        media_url_expr = parse("$..media_url_https")
        like_count_expr = parse("$..legacy.favorite_count")
        retweet_count_expr = parse("$..legacy.retweet_count")

        # Use the expressions to find the values
        try:
            tweet_id = [match.last_value for match in tweet_id_expr.find(tweet_dict)][0]
            screen_name = [match.last_value for match in screen_name_expr.find(tweet_dict)][0]
            followers_count = [match.last_value for match in followers_count_expr.find(tweet_dict)][0]
            url_screen_name = [match.last_value for match in url_expr.find(tweet_dict)][0]  # used to build URL
            text = [match.last_value for match in text_expr.find(tweet_dict)][0]
            timestamp_str = [match.last_value for match in timestamp_expr.find(tweet_dict)][0]
            image_url = [match.last_value for match in image_url_expr.find(tweet_dict)][0]
            media_urls = [match.last_value for match in media_url_expr.find(tweet_dict)]
            like_count = [match.last_value for match in like_count_expr.find(tweet_dict)][0]
            retweet_count = [match.last_value for match in retweet_count_expr.find(tweet_dict)][0]

            # Convert the timestamp to a datetime object
            timestamp = dt.datetime.strptime(timestamp_str, '%a %b %d %H:%M:%S %z %Y')

            # Construct the URL
            url = f"https://twitter.com/{url_screen_name}/status/{tweet_id}"

            return {
                "timestamp": timestamp,
                "tweet_id": tweet_id,
                "screen_name": screen_name,
                "followers_count": followers_count,
                "url": url,
                "text": text.replace('\n', ' '),
                "image_url": image_url,
                "media_urls": media_urls,
                "like_count": like_count,
                "retweet_count": retweet_count
            }
        except:
            return None


async def query_twitter(username, password, email, email_pw, user_id):
    pool = AccountsPool()
    await pool.add_account(username, password, email, email_pw)
    await pool.login_all()
    api = API(pool)

    user = await api.user_by_id(user_id)
    user_tweets = []
    async for tweet in api.user_tweets(user_id, limit=20):
        user_tweets.append(tweet)
    return user, user_tweets


def get_tweet_attributes(tweet):
    tweet = tweet.to_dict()
    tweet_attributes = {
        "date": tweet['date'],
        "replyCount": tweet['replyCount'],
        "retweetCount": tweet['retweetCount'],
        "likeCount": tweet['likeCount'],
        "quoteCount": tweet['quoteCount'],
        "viewCount": tweet['viewCount'],
        "likeCount": tweet['likeCount'],
    }
    return tweet_attributes


def get_user_stats(user):
    stats = {
        'id': user.id,
        'url': user.url,
        'username': user.username,
        'display_name': user.displayname,
        'created': user.created,
        'followers_count': user.followersCount,
        'friends_count': user.friendsCount,
        'statuses_count': user.statusesCount,
        'favourites_count': user.favouritesCount,
        'listed_count': user.listedCount,
    }
    return stats
