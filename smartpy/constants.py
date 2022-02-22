import smartpy.utility.os_util as os_util
import boto3
import os

CURRENT_DIR = os_util.getCurrentDirPath()
GLOBAL_BOTO3_SESSION = boto3.Session()


# Market data
BINANCE = 'binance'
# Providers
BINANCE = 'binance'
BINANCE_FUTURES = 'binance_futures'
YAHOO = 'yahoo'
# Symbols
BTCUSDT = 'BTCUSDT'
DOGEUSDT = 'DOGEUSDT'
# OHLC intervals
OHLC_INTERVAL_1M = '1m'
OHLC_INTERVAL_5M = '5m'
OHLC_INTERVAL_30M = '30m'
OHLC_INTERVAL_1H = '1h'
OHLC_INTERVAL_1D = '1d'
OHLC_INTERVAL_1W = '1w'
