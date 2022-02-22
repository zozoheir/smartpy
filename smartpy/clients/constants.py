from binance.enums import *

# In Binance futures, for a given leverage there is a maximum notional allowed to be invested
LEVERAGE_NOTIONAL_RULE = {'125x': {'125': 400, '100': 2500, '50': 20000, '20': 500000, '10': 2000000,
                                   '5': 10000000, '4': 25000000, '3': 66666666, '2': 150000000, '1': 500000000},
                          '100x': {'100': 100, '75': 1333, '50': 10000, '25': 40000, '10': 200000,
                                   '5': 1000000, '4': 2500000, '3': 6666666, '2': 10000000, '1': 500000000},
                          '75x': {'75': 133, '50': 1000, '25': 10000, '10': 100000, '5': 400000, '4': 1250000,
                                  '3': 3333333, '2': 5000000, '1': 500000000},
                          '50x': {'50': 100, '20': 1250, '10': 10000, '5': 5000, '2': 500000, '1': 500000000}}
CURRENCY_LEVERAGE_MAP = {'DOGEUSDT': '50x', 'LTCUSDT': '75x', 'XRPUSDT': '75x', 'BTCUSDT': '125x', 'ETHUSDT': '100x'}

# Binance sym_orders_df
MARKET = ORDER_TYPE_MARKET
TRAILING_STOP_MARKET = 'TRAILING_STOP_MARKET'
STOP_MARKET = 'STOP_MARKET'
GTC = TIME_IN_FORCE_GTC
LAST_TRADE_PRICE = 'CONTRACT_PRICE'
STATUS_CANCELED = 'CANCELED'
STATUS_NEW = 'NEW'
STATUS_FILLED = 'FILLED'
STATUS_PARTIALLY_FILLED = 'PARTIALLY_FILLED'
