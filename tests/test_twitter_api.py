import unittest
import smartpy.twitter.twitter_api as twitter
from smartpy.aws.s3 import S3

s3 = S3()

N_TWEET_TESTS = 10
twitter_api = twitter.Twitter()


class TestTwitterAPI(unittest.TestCase):

    def test_search(self):

        search_params = {
            "from": 'elonmusk',
            "since": '2021-06-01',
            "until":'2021-07-01'
        }
        twitter_search_results = twitter_api.search(search_params)
        self.assertEqual(len(twitter_search_results), 214, "Twitter search returning different results than expected")

        search_params = {
            "from": ['elonmusk',"ptj_official"],
            "since": '2021-06-01',
            "until": '2021-07-01'
        }
        twitter_search_results = twitter_api.search(search_params)
        self.assertEqual(len(twitter_search_results), 217, "Twitter search returning different results than expected")


if __name__ == '__main__':
    unittest.main()
