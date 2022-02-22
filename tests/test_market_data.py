import unittest

import smartpy.data.market_data as market_data
from smartpy.constants import *


BINANCE_API_KEYS = {'binance': {'api_key': 'pBhiobD37b8BnX0jn1JoOZkXVka3T4xtxyTqEvGQzvitLHemWQjJFikDa7b7UyVE',
                                'api_secret': 'NxVL1uozIvJ24CMmxkBhQ7XtQwQBk1XenHM1bsDr4Z4bWErokKP8dZzB4IetSlm5'
                                }
                    }

md = market_data.MarketData(BINANCE_API_KEYS, single_provider= BINANCE)


class TestMarketData(unittest.TestCase):

    def test_correctMarketData(self):
        df = md.getHistoricalOHLC(symbol='DOGEUSDT', interval='1m', start_time='2021-04-23', end_time='2021-04-24')
        df = df[['timestamp','close']]
        is_correct_price = 0.169 <= df.set_index('timestamp').between_time('02:25', '02:26').max().values[0] <= 0.171
        self.assertTrue(is_correct_price)

if __name__ == '__main__':
    unittest.main()
