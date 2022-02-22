import datetime as dt
from decimal import Decimal
import math
import numpy as np
import pandas as pd
# from order_book import OrderBook

from binance.enums import *

BINANCE_API_V1_URL = "https://api.binance.com/sapi/v1"
BINANCE_OB_HISTORICAL_DATA_URL = "{}/futuresHistDataId".format(BINANCE_API_V1_URL)

#ob_object = OrderBook()
unix_to_utc = lambda x: dt.datetime.fromtimestamp(int(x) / 1000, dt.timezone.utc)


class BinanceClient:

    def __init__(self, api_key, api_secret):
        from binance.client import Client
        self.client = Client(api_key, api_secret)

    def getSymbols(self):
        return [i['symbol'] for i in self.client.get_all_tickers()]

    def getStepSize(self, symbol):
        symbol_info = self.client.get_symbol_info(symbol)
        step_size = list(filter(lambda x: (x['filterType'] == 'LOT_SIZE'), symbol_info['filters']))[0]['stepSize']

        return step_size

    def getTickSize(self, symbol):
        symbol_info = self.client.get_symbol_info(symbol)
        tick_size = list(filter(lambda x: (x['filterType'] == 'PRICE_FILTER'), symbol_info['filters']))[0]['tickSize']
        return tick_size


    # def getOB(self, symbol) -> {Decimal: float}:
    #     res = self.telegram_client.get_order_book(symbol=symbol)
    #     price_size_bids = res['bids']
    #     price_size_asks = res['asks']
    #     ob_object.bids = {Decimal(price): size for (price, size) in price_size_bids}
    #     ob_object.asks = {Decimal(price): size for (price, size) in price_size_asks}
    #     return ob_object.bids, ob_object.asks

    def getRecentTrades(self, symbol):
        trades_cols_to_keep = ['T', 'p', 'q', 'm']
        agg_trades = self.binance_client.aggregate_trade_iter(symbol=symbol, start_str='30 minutes')
        trades_data = pd.DataFrame([i for i in agg_trades])[trades_cols_to_keep]
        trades_data = trades_data.rename({'p': 'price',
                                          'q': 'qty',
                                          'T': 'timestamp',
                                          'm': 'is_buy'}, axis=1)
        trades_data['timestamp'] = trades_data['timestamp'].apply(lambda x: dt.datetime.fromtimestamp(int(x) / 1000))
        return trades_data

    def getRecentOHLC(self, symbol, interval, volume_data=False, ln=False):
        """
        Returns processed recent klines
        """
        recent_data_list = self.client.get_klines(symbol=symbol, interval=interval)
        final_df = self._processKlinesData(recent_data_list, volume_data=False, ln=False)
        return final_df

    def getHistoricalOHLC(self, symbol, interval, start_time, end_time, volume_data=False, ln=False):
        """
        Returns processed historical klines
        """
        historical_data_list = self.client.get_historical_klines(symbol=symbol, interval=interval,
                                                                 start_str=str(start_time), end_str=str(end_time))

        final_df = self._processKlinesData(historical_data_list, volume_data=False, ln=False)
        final_df['timestamp'] = final_df['timestamp'].dt.round('1min')
        return final_df

    def getHistoricalDFFromList(self, data_list):
        df = pd.DataFrame(data_list)
        df.columns = ['opents', 'open', 'high', 'low', 'close', 'volume', 'timestamp', 'none', 'n_trades',
                      'buy_volume',
                      'none', 'none']
        df = df.astype(float)
        df['timestamp'] = df['timestamp'].apply(unix_to_utc)
        cols_to_return = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'n_trades', 'buy_volume']
        return df[cols_to_return]

    def _processKlinesData(self, data: list, volume_data=False, ln=False):

        final_df = self.getHistoricalDFFromList(data)
        final_df = final_df.drop_duplicates(subset='timestamp').sort_values('timestamp')
        final_df['hour'] = final_df['timestamp'].dt.hour
        final_df['day_of_week'] = final_df['timestamp'].dt.dayofweek + 1
        # We include Rt and Rt+1
        final_df['r_t'] = final_df['close'] / final_df['open'] - 1
        final_df['r_t+1'] = final_df['r_t'].shift(-1)
        final_df['sell_volume'] = final_df['volume'] - final_df['buy_volume']
        final_df['order_imbalance_raw'] = final_df['buy_volume'] - final_df['sell_volume']
        final_df['order_imbalance_rel'] = final_df['order_imbalance_raw'] / final_df['volume']
        return final_df.dropna()

    def placeMarketOrder(self, symbol, side, quantity):

        order_type = ORDER_TYPE_MARKET
        return self.client.create_order(symbol=symbol, side=side, type=order_type, quantity=quantity)

    @staticmethod
    def getHistoricalDFFromList(data_list):
        df = pd.DataFrame(data_list)
        df.columns = ['opents', 'open', 'high', 'low', 'close', 'volume', 'timestamp', 'none', 'n_trades',
                      'buy_volume',
                      'none', 'none']
        df = df.astype(float)
        df['timestamp'] = df['timestamp'].apply(unix_to_utc)
        cols_to_return = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'n_trades', 'buy_volume']
        return df[cols_to_return]

    @staticmethod
    def floatPrecision(f, n):
        n = int(math.log10(1 / float(n)))
        f = math.floor(float(f) * 10 ** n) / 10 ** n
        f = "{:0.0{}f}".format(float(f), n)
        return str(int(f)) if int(n) == 0 else f

