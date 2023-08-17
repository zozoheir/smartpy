import smartpy.utility.dt_util as dt_util
import datetime as dt
import logging
from decimal import Decimal
from functools import wraps
from time import time

import numpy as np
import pandas as pd

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

MINUTES_IN_INTERVAL = {OHLC_INTERVAL_1M: 1,
                       OHLC_INTERVAL_5M: 5,
                       OHLC_INTERVAL_30M: 30,
                       OHLC_INTERVAL_1H: 60,
                       OHLC_INTERVAL_1D: 60 * 24,
                       OHLC_INTERVAL_1W: 60 * 24 * 7
                       }
INTERVALS_PER_DAY = {OHLC_INTERVAL_1M: 60 * 24,
                     OHLC_INTERVAL_5M: 12 * 24,
                     OHLC_INTERVAL_30M: 6 * 24,
                     OHLC_INTERVAL_1H: 24,
                     OHLC_INTERVAL_1D: 1,
                     OHLC_INTERVAL_1W: 7
                     }


class MarketData:
    """
    - Query tests and historical market data from various sources.
    - Option to use a single provider, or leave it open to getSignals provider on the fly
    - We define simple functions (getOB, getUserOrderFlow) and build other compute related functions (VWAP,
    mid, spread calculation etc) from those
    """

    def __init__(self, api_keys: dict, single_provider=None):

        self.single_provider = single_provider
        self.all_clients = {}

        if self.single_provider:
            if self.single_provider == BINANCE and BINANCE in api_keys.keys():
                from smartpy.clients.binance_client import BinanceClient
                api_key = api_keys[BINANCE]['api_key']
                api_secret = api_keys[BINANCE]['api_secret']
                self.single_client = BinanceClient(api_key, api_secret)
                self.binance_client = self.single_client
            elif self.single_provider == BINANCE_FUTURES and BINANCE_FUTURES in api_keys.keys():
                from smartpy.clients.binance_futures_client import BinanceFuturesClient
                api_key = api_keys[BINANCE]['api_key']
                api_secret = api_keys[BINANCE]['api_secret']
                self.single_client = BinanceFuturesClient(api_key, api_secret)
                self.binance_futures_client = self.single_client

        else:

            if BINANCE in api_keys.keys():
                from smartpy.clients.binance_client import BinanceClient
                api_key = api_keys[BINANCE]['api_key']
                api_secret = api_keys[BINANCE]['api_secret']
                self.binance_client = BinanceClient(api_key,
                                                    api_secret)
            if BINANCE_FUTURES in api_keys.keys():
                from smartpy.clients.binance_futures_client import BinanceFuturesClient
                api_key = api_keys[BINANCE_FUTURES]['api_key']
                api_secret = api_keys[BINANCE_FUTURES]['api_secret']
                self.binance_futures_client = BinanceFuturesClient(api_key,
                                                                   api_secret)

    def print_exec_time(func):
        @wraps(func)
        def _time_it(*args, **kwargs):
            start = int(round(time() * 1000))
            try:
                return func(*args, **kwargs)
            finally:
                end_ = int(round(time() * 1000)) - start
                logger.info(f"Exec time: {end_ if end_ > 0 else 0} ms")

        return _time_it

    def getSymbols(self, provider=None):
        if self.single_provider:
            return self.single_client.getSymbols()
        elif self.single_provider is None:
            if provider == BINANCE:
                return self.binance_client.getSymbols()

    def getHistoricalOHLC(self,
                          provider=None,
                          symbol=None,
                          interval='1d',
                          start_time='2020-01-01',
                          end_time='2020-01-02',
                          volume_data=False,
                          ln=False,
                          data_quality_cutoff=None) -> pd.DataFrame:
        if start_time==end_time:
            end_time = dt_util.formatDatetime(dt_util.toDatetime(end_time) + dt.timedelta(days=1), dt_util.FMT_YMD_DASH)
        df_to_return = None
        if self.single_provider:
            df_to_return = self.single_client.getHistoricalOHLC(symbol, interval, start_time, end_time, volume_data, ln)
        elif self.single_provider is None:
            if provider == BINANCE:
                df_to_return = self.binance_client.getHistoricalOHLC(symbol, interval, start_time, end_time,
                                                                     volume_data, ln)
            if provider == BINANCE_FUTURES:
                df_to_return = self.binance_futures_client.getHistoricalOHLC(symbol, interval, start_time, end_time,
                                                                             volume_data, ln)

        # Check points with missing data
        df_to_return['time_diff_min'] = df_to_return['timestamp'].diff(1).astype('timedelta64[m]')
        missing_points = df_to_return[df_to_return['time_diff_min'] != MINUTES_IN_INTERVAL[interval]]
        expected_days_in_timeframe = (
                dt.datetime.strptime(end_time, '%Y-%m-%d') - dt.datetime.strptime(start_time, '%Y-%m-%d')).days

        expected_points_in_timeframe = max(expected_days_in_timeframe,1) * INTERVALS_PER_DAY[interval]

        percentage_points_missing = len(missing_points) / expected_points_in_timeframe
        print(
            '{} {} to {} : data quality get_check_results - {:.1%} of expected data points missing'.format(symbol, start_time,
                                                                                               end_time,
                                                                                               percentage_points_missing))
        if data_quality_cutoff:
            assert percentage_points_missing < data_quality_cutoff, Exception('Poor data quality')

        return df_to_return

    @print_exec_time
    def getOB(self, provider=None, symbol=None) -> {Decimal: float}:
        if self.single_provider:
            return self.single_client.getOB(symbol)
        elif self.single_provider is None:
            if provider == BINANCE:
                return self.binance_client.getOB(symbol)
            elif provider == BINANCE_FUTURES:
                return self.binance_futures_client.getOB(symbol)

    def getRecentTrades(self, provider=None, symbol=None):
        if self.single_provider:
            return self.single_client.getRecentTrades(self, symbol)
        elif self.single_provider is None:
            if provider == BINANCE:
                return self.binance_client.getRecentTrades(symbol)
            elif provider == BINANCE_FUTURES:
                return self.binance_futures_client.getRecentTrades(symbol)

    """
    All functions below need not be changed as they all use the native functions above 
    """

    def getMid(self, provider=None, symbol=None):
        bids, asks = self.getOB(provider, symbol)
        mid = Decimal(1 / 2) * (asks.index(0)[0] + bids.index(0)[0])
        return mid

    def getTOB(self, provider=None, symbol=None):
        bids, asks = self.getOB(provider, symbol)
        return bids.index(0)[0], asks.index(0)[0]

    def getSpreadRaw(self, provider=None, symbol=None):
        bid, ask = self.getTOB(provider, symbol)
        return ask - bid

    def getSpreadBps(self, provider=None, symbol=None):
        bid, ask = self.getTOB(provider, symbol)
        spread = ask - bid
        mid = Decimal(1 / 2) * (ask + bid)
        return spread / mid * 10000

    def _getVWAP(self, order_size, ob_list):
        sizes = np.array([])
        prices = np.array([])
        cumsize = Decimal(0)
        for i in range(len(ob_list)):
            price_size_pair = ob_list.index(i)
            price = price_size_pair[0]
            size = Decimal(price_size_pair[1])
            prices = np.append(prices, [price])
            sizes = np.append(sizes, [size])
            cumsize += size
            if cumsize >= order_size:
                weights = sizes / cumsize
                wavg_price = np.average(prices, weights=weights)
                return wavg_price

    def getOrderSpreadRaw(self, provider, symbol, order_size):
        bids, asks = self.getOB(provider, symbol)
        vwap_bid = self._getVWAP(order_size, bids)
        vwap_ask = self._getVWAP(order_size, asks)
        return vwap_ask - vwap_bid

    def getOrderSpreadBps(self, provider, symbol, order_size):
        bids, asks = self.getOB(provider, symbol)
        vwap_bid = self._getVWAP(order_size, bids)
        vwap_ask = self._getVWAP(order_size, asks)
        mid = Decimal(1 / 2) * (vwap_ask + vwap_bid)
        return (vwap_ask - vwap_bid) / mid * 10000
