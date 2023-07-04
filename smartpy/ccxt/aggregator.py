import ccxt
import pandas as pd

from smartpy.ccxt.helpers import CCXT_EXCEPTIONS
from smartpy.utility.log_util import getLogger
from smartpy.utility.py_util import keep_trying
import smartpy.utility.dt_util as dt_util
import datetime as dt


logger = getLogger(__name__)

MINIMAL_ORDER_COLUMNS = ['timestamp', 'coin', 'filled', 'side', 'price', 'status']
CCXT_OHLC_HEADERS = ['timestamp', 'open', 'high', 'low', 'close', 'volume']

minutes_add = {
    "1m": 1,
    "5m": 5,
    "1h": 60,
    "1d": 60 * 24
}


class CCXTAggregator:

    def __init__(self,
                 config):
        self.exchange_balances = {}
        self.exchange_orders = {}

        self.exchange_list = config.keys()
        self.exchange_live_market_data = {}
        self.ccxt_exchange_objects = {}
        self.exchange_markets = {}

        # Initilizing various objects
        for exchange in self.exchange_list:
            exchange_class = getattr(ccxt, exchange)
            self.ccxt_exchange_objects[exchange] = exchange_class(config[exchange])
            self.exchange_markets[exchange] = self.ccxt_exchange_objects[exchange].load_markets()

    @keep_trying(exceptions=CCXT_EXCEPTIONS)
    def getAvailableSymbols(self,
                            exchange,
                            quoted=''):
        return [i for i in self.exchange_markets[exchange].keys() if i.endswith(quoted)]

    @keep_trying(exceptions=CCXT_EXCEPTIONS)
    def get_ohlc(self,
                 exchange,
                 symbol,
                 timeframe,
                 start_time,
                 end_time=str(dt.datetime.now())):

        # standardize to dateitme
        start_time = dt_util.toDatetime(start_time)
        end_time = dt_util.toDatetime(end_time)
        # Put the right format so it doesn't bug
        start_time = str(start_time.date()) + ' 00:00:00'
        end_time = str(end_time)

        exchange_object = self.ccxt_exchange_objects[exchange]
        from_timestamp = exchange_object.parse8601(start_time)
        msec = 1000
        minute = 60 * msec
        now = exchange_object.milliseconds()
        data = []
        while from_timestamp < now:
            ohlcvs = exchange_object.fetch_ohlcv(symbol, timeframe, from_timestamp)
            if len(ohlcvs) > 0:
                from_timestamp = ohlcvs[-1][0] + minute * minutes_add[timeframe]
                data += ohlcvs
                # Stop when you get to end time + 1 day
                if from_timestamp > exchange_object.parse8601(str(end_time)) + 1000 * 3600 * 24:
                    break
            else:
                from_timestamp += 1000 * 3600 * 24 * 7

        df = pd.DataFrame(data, columns=CCXT_OHLC_HEADERS)
        df['timestamp'] = pd.to_datetime(df.timestamp, unit='ms')
        df['coin'] = symbol.split('/')[0]
        return df

    @keep_trying(exceptions=CCXT_EXCEPTIONS)
    def get_last(self,
                 exchange,
                 symbol,
                 translator_ccy='BTC'):
        if symbol in self.getAvailableSymbols(exchange):
            return self.exchange_live_market_data[exchange].get_last(symbol)
        else:
            logger.info(f"{symbol} not available on {exchange}, need to calculate syntethic through {translator_ccy}")
            return self.getSyntheticRate(exchange, symbol, translator_ccy)

    @keep_trying(exceptions=CCXT_EXCEPTIONS)
    def getSyntheticRate(self,
                         exchange,
                         symbol,
                         translator_ccy):
        split_symbol = symbol.split('/')
        symbol_base_ccy, symbol_quoted_ccy = split_symbol[0], split_symbol[1]

        symbol_base_vs_translator = f"{symbol_base_ccy}/{translator_ccy}"
        symbol_base_vs_translator_live_rate = \
            self.ccxt_exchange_objects[exchange].fetch_ticker(symbol=symbol_base_vs_translator)['last']

        translator_vs_symbol_quoted = f"{translator_ccy}/{symbol_quoted_ccy}"
        translator_vs_symbol_quoted_live_rate = \
            self.ccxt_exchange_objects[exchange].fetch_ticker(symbol=translator_vs_symbol_quoted)['last']

        symbol_live_rate = symbol_base_vs_translator_live_rate * translator_vs_symbol_quoted_live_rate
        return symbol_live_rate

    @keep_trying(exceptions=CCXT_EXCEPTIONS)
    def getBalances(self, exchange):
        return self.ccxt_exchange_objects[exchange].fetchBalance()

    @keep_trying(exceptions=CCXT_EXCEPTIONS)
    def getMyTrades(self,
                    exchange,
                    symbols):
        """
        Get fills
        :param exchange:
        :param coin:
        :return:
        """
        if isinstance(symbols, str):
            symbols = [symbols]
        trades = pd.DataFrame()
        for sym in symbols:
            tmp = pd.DataFrame(self.ccxt_exchange_objects[exchange].fetchMyTrades(symbol=sym))
            trades = trades.append(tmp)
        return trades.reset_index(drop=True)

    def getClosedOrders(self,
                        exchange,
                        symbol,
                        start_time,
                        end_time):

        all_closed_orders = []
        page = 1

        while True:
            since = None
            limit = 1000
            params = {
                'title': int((self.ccxt_exchange_objects[exchange].parse8601(str(start_time))) / 1000),
                'to': int((self.ccxt_exchange_objects[exchange].parse8601(str(end_time))) / 1000),
                'page': page,
                'limit': 1000
            }
            closed_orders = self.ccxt_exchange_objects[exchange].fetchClosedOrders(symbol=symbol,
                                                                                   since=since,
                                                                                   limit=limit,
                                                                                   params=params)
            all_closed_orders += closed_orders

            if len(closed_orders):
                page += 1
            else:
                break
        closed_orders_df = pd.DataFrame(all_closed_orders)
        if len(closed_orders_df)>0:
            return processGateIOCCXTOrdersDF(closed_orders_df)
        else:
            closed_orders_df

    @keep_trying(exceptions=CCXT_EXCEPTIONS)
    def getOpenOrders(self, exchange):
        """
        Get current open orders
        :param exchange:
        :return:
        """
        df = pd.DataFrame(self.ccxt_exchange_objects[exchange].fetchOpenOrders())
        if len(df) > 0:
            df['coin'] = df['coin'].apply(lambda x: x.split('/')[0])
            if len(df) > 0:
                df['datetime'] = pd.DatetimeIndex(df['datetime']).tz_convert('EST')
            return df
        else:
            return pd.DataFrame()

    @keep_trying(exceptions=CCXT_EXCEPTIONS)
    def getAllOrders(self, exchange, symbol, since):
        """
        Get all orders, open or not
        :param exchange:
        :param symbol:
        :param since:
        :return:
        """
        # TODO https://docs.ccxt.com/en/latest/manual.html#overriding-unified-api-params
        exchange_object = self.ccxt_exchange_objects[exchange]
        since = round(dt_util.toDatetime(since).timestamp() * 1000)
        orders = exchange_object.fetch_orders(symbol=symbol,
                                              since=since)
        if len(orders) > 0:
            orders_df = pd.DataFrame(orders)
            orders_df['timestamp'] = pd.DatetimeIndex(orders_df['timestamp']).tz_localize('UTC').tz_convert('EST')
            return orders_df
        else:
            return None

    def sendAgressiveOrder(self,
                           exchange,
                           symbol,
                           side,
                           base_amount=None,
                           usd_amount=None,
                           aggressivity_bps=500,
                           params={}):
        if side == 'sell':
            agressivity = (1 - aggressivity_bps / 10000)
        elif side == 'buy':
            agressivity = (1 + aggressivity_bps / 10000)

        mid = self.getMid(exchange=exchange,
                          symbol=symbol)

        # Set local equivalent based on which input CCY was used
        if usd_amount is None:
            local_equivalent = base_amount
            usd_amount = 0
        elif base_amount is None:
            local_equivalent = usd_amount / mid

        message = f"Agressive limit {side} of {round(usd_amount)} USD / {local_equivalent} {symbol.split('/')[0]}"
        logger.info(message)

        entry_order = self.ccxt_exchange_objects[exchange].create_order(
            symbol=symbol,
            type='limit',
            side=side,
            amount=local_equivalent,
            price=mid * agressivity,
            params=params)

        return entry_order