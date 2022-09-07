import datetime as dt
import numpy as np
import pandas as pd
from time import sleep
# from order_book import OrderBook
from pytz import timezone
from binance.exceptions import BinanceAPIException
import logging
from binance.client import Client

from smartpy.clients.constants import *
from smartpy.quant.arithmetic import numeralWithPrecision

# ob_object = OrderBook()
utc = timezone('UTC')

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s - %(name)s', level=logging.INFO)
logger = logging.getLogger(__name__)


class BinanceFuturesClient:

    def __init__(self, api_key, api_secret, telegram_logs=None):
        self.api_key = api_key
        self.api_secret = api_secret
        self.client = Client(api_key, api_secret)
        self.telegram_logs = telegram_logs

    def testConnection(self):
        try:
            self.client.get_all_tickers()
        except Exception as e:
            logger.warning(f'Reset Binance connection. {type(e)}. {e}')
            self.client = Client(self.api_key, self.api_secret)

    def getSymbols(self):
        self.testConnection()

        return [i['coin'] for i in self.client.get_all_tickers()]

    # def getOB(self, coin):
    #     res = self.telegram_client.futures_order_book(coin=coin)
    #     price_size_bids = res['bids']
    #     price_size_asks = res['asks']
    #     ob_object.bids = {Decimal(price): size for (price, size) in price_size_bids}
    #     ob_object.asks = {Decimal(price): size for (price, size) in price_size_asks}
    #     return ob_object.bids, ob_object.asks

    def getStepSize(self, symbol):
        self.testConnection()
        exchange_info = self.client.futures_exchange_info()
        symbol_info = list(filter(lambda x: (x['coin'] == symbol), exchange_info['cryptofeed_symbols']))
        step_size = list(filter(lambda x: (x['filterType'] == 'LOT_SIZE'), symbol_info[0]['filters']))[0]['stepSize']

        return step_size

    def getTickSize(self, symbol):
        self.testConnection()
        exchange_info = self.client.futures_exchange_info()
        symbol_info = list(filter(lambda x: (x['coin'] == symbol), exchange_info['cryptofeed_symbols']))
        tick_size = list(filter(lambda x: (x['filterType'] == 'PRICE_FILTER'), symbol_info[0]['filters']))[0]['tickSize']

        return tick_size

    def getMinQty(self, symbol):
        self.testConnection()
        exchange_info = self.client.futures_exchange_info()
        symbol_info = list(filter(lambda x: (x['coin'] == symbol), exchange_info['cryptofeed_symbols']))
        min_qty = list(filter(lambda x: (x['filterType'] == 'LOT_SIZE'), symbol_info[0]['filters']))[0]['minQty']

        return min_qty

    def getMinNotional(self, symbol):
        self.testConnection()
        exchange_info = self.client.futures_exchange_info()
        symbol_info = list(filter(lambda x: (x['coin'] == symbol), exchange_info['cryptofeed_symbols']))
        min_notional = list(filter(lambda x: (x['filterType'] == 'MIN_NOTIONAL'), symbol_info[0]['filters']))[0]['notional']

        return min_notional

    def getRecentTrades(self, symbol):
        trades_cols_to_keep = ['T', 'p', 'q', 'm']
        agg_trades = self.client.futures_aggregate_trades(symbol=symbol, start_str='30 minutes_add ago UTC')
        trades_data = pd.DataFrame([i for i in agg_trades])[trades_cols_to_keep]
        trades_data = trades_data.rename({'p': 'price',
                                          'q': 'qty',
                                          'T': 'timestamp',
                                          'm': 'is_buy'}, axis=1)
        trades_data['timestamp'] = trades_data['timestamp'].apply(lambda x: dt.datetime.fromtimestamp(int(x) / 1000))

        return trades_data

    def getLastPrice(self, symbol):
        self.testConnection()
        last_price = self.client.futures_aggregate_trades(symbol=symbol, limit=1)[0]['p']

        return last_price

    def getHistoricalOHLC(self, symbol, interval, start_time, end_time, volume_data, ln):
        """
        Returns OHLC data with timestamps from wanted_timezone of requester (EST for Montreal etc...)
        """
        self.testConnection()
        historical_data_list = self.client.futures_klines(symbol=symbol, interval=interval, start_time=str(start_time),
                                                          end_time=str(end_time))
        final_df = self.getHistoricalDFFromList(historical_data_list)
        final_df = final_df.drop_duplicates(subset='timestamp').sort_values('timestamp')
        final_df['hour'] = final_df['timestamp'].dt.hour
        final_df['day_of_week'] = final_df['timestamp'].dt.dayofweek + 1
        final_df['timestamp'] = final_df['timestamp'].apply(lambda x: utc.localize(x))

        # We include Rt and Rt+1 (ln or normal version)
        if ln:
            final_df['r_t'] = final_df['close'].shift(1) / final_df['open'].shift(1) - 1
            final_df['r_t+1'] = final_df['close'].shift(-1) / final_df['open'].shift(-1) - 1
        else:
            final_df['r_t'] = np.log(final_df['close'].shift(1) / final_df['open'].shift(1))
            final_df['r_t+1'] = np.log(final_df['close'].shift(-1) / final_df['open'].shift(-1))

        if volume_data:
            final_df['sell_volume'] = final_df['volume'] - final_df['buy_volume']
            final_df['order_imbalance_raw'] = final_df['buy_volume'] - final_df['sell_volume']
            final_df['order_imbalance_rel'] = final_df['order_imbalance_raw'] / final_df['volume']
        return final_df.dropna()

    @staticmethod
    def getHistoricalDFFromList(data_list):
        df = pd.DataFrame(data_list)
        df.columns = ['opents', 'open', 'high', 'low', 'close', 'volume', 'timestamp', 'none', 'n_trades',
                      'buy_volume',
                      'none', 'none']
        df = df.astype(float)
        df['timestamp'] = df['timestamp'].apply(lambda x: dt.datetime.fromtimestamp(x / 1000))
        cols_to_return = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'n_trades', 'buy_volume']

        return df[cols_to_return]

    def checkLeverageValidity(self, leverage, symbol, notional):
        # Make sure leverage is an integer
        is_leverage_valid = True
        error_msg = None
        try:
            leverage = int(leverage)
        except ValueError:
            is_leverage_valid = False
            error_msg = 'Leverage needs to be an integer.'

            return is_leverage_valid, error_msg

        # Load Binance trading rules w.r.t leverage
        currency_leverage_map = CURRENCY_LEVERAGE_MAP[symbol]
        leverages = [int(i) for i in LEVERAGE_NOTIONAL_RULE[currency_leverage_map].keys()]
        closest_lower_leverage = min([i for i in leverages if i <= leverage], key=lambda x: abs(x - leverage))
        estimated_qty = str(numeralWithPrecision(float(notional) * float(leverage) / float(
                                                      self.getLastPrice(symbol)), self.getStepSize(symbol)))

        # Make sure leverage value is allowed
        if leverage > max(leverages):
            is_leverage_valid = False
            error_msg = f'This Leverage is higher than the maximum allowed for this currency. ' \
                        f'The maximum Leverage allowed is {max(leverages)}.'

        # Make sure resulting position size is allowed given the leverage
        elif float(notional) * leverage > LEVERAGE_NOTIONAL_RULE[currency_leverage_map][str(closest_lower_leverage)]:
            is_leverage_valid = False
            error_msg = f'The choice of Notional and Leverage results in a leveraged position not allowed for ' \
                        f'this currency. For this Leverage the maximum Notional allowed is ' \
                        f'{LEVERAGE_NOTIONAL_RULE[currency_leverage_map][str(closest_lower_leverage)]}.'

        # Make sure resulting quantity is above the minimum
        elif float(estimated_qty) < float(self.getMinQty(symbol)) or float(notional) * leverage < float(self.getMinNotional(symbol)):
            is_leverage_valid = False
            error_msg = f"Resulting quantity and/or position size lower than the minimum allowed for this currency. " \
                        f"Min quantity should be {self.getMinQty(symbol)} and " \
                        f"minimum position size {self.getMinNotional(symbol)} USDT."

        return is_leverage_valid, error_msg

    def changeMarginType(self, symbol, margin_type):
        self.testConnection()
        while True:
            try:
                self.client.futures_change_margin_type(symbol=symbol, marginType=margin_type)
                break
            except BinanceAPIException as e:
                if e.message == 'No need to change margin type.':
                    break
                else:
                    error_msg = 'Something went wrong while enforcing isolated mode. Please check.'
                    if self.telegram_logs is not None:
                        self.telegram_logs.sendMsg(error_msg)
                    logger.error(error_msg)

    def placeMarketOrder(self, symbol, side, quantity):
        self.testConnection()
        order_type = MARKET

        return self.client.futures_create_order(symbol=symbol, side=side, type=order_type, quantity=quantity)

    # We assume we only trade coins_metadata paired with usdt for now
    def fillMarketOrder(self, symbol, side, notional_usdt, leverage, margin_type='ISOLATED'):
        logger.info('Placing order to {} {} USDT worth of {} @ MARKET'.format(side, notional_usdt, symbol))
        self.testConnection()
        executed_order = {'ORDER_ID': None, 'QUANTITY': None, 'PRICE': None}

        # Binance doesn't support notional-based api_response_trades_list for futures. Need to convert notional to quantity.
        last_price = self.getLastPrice(symbol)
        quantity = str(numeralWithPrecision(float(notional_usdt) * float(leverage) / float(last_price),
                       self.getStepSize(symbol)))

        # Enforce margin type
        self.changeMarginType(symbol, margin_type)

        # Update leverage at the coin level
        self.client.futures_change_leverage(symbol=symbol, leverage=leverage)

        # Place the order
        try:
            placed_order = self.placeMarketOrder(symbol, side, quantity)
        except Exception as e:
            if self.telegram_logs is not None:
                self.telegram_logs.sendMsg('Exception: {}'.format(str(e)))
            logger.error('Exception: {}'.format(str(e)))

            return {}

        # Make sure order gets filled
        # Initialize failed attempts counter
        i = 0
        while True:
            processed_order = self.client.futures_get_order(symbol=symbol, orderId=placed_order['orderId'])
            if processed_order['status'] == STATUS_FILLED:
                executed_order = {'ORDER_ID': placed_order['orderId'], 'QUANTITY': processed_order['executedQty'],
                                  'PRICE': float(processed_order['avgPrice'])}
                actual_notional = round(float(processed_order['avgPrice']) * float(executed_order['QUANTITY']) / float(leverage), 1)
                message = 'Order {} was filled: {} {} @ {}. Exact notional invested: {}'.format(executed_order['ORDER_ID'],
                                                                                                float(executed_order['QUANTITY']), symbol, executed_order['PRICE'],
                                                                                                actual_notional)
                if self.telegram_logs is not None:
                    self.telegram_logs.sendMsg('*****NEW TRADE*****')
                    self.telegram_logs.sendMsg(message)
                logger.info(message)

                return executed_order

            elif processed_order['status'] in (STATUS_PARTIALLY_FILLED, STATUS_NEW):
                logger.info(f"Order {placed_order['orderId']} not filled yet. Status: {processed_order['status']}")
                sleep(2)
            else:
                i += 1
                if i > 4:
                    message = f"Order {placed_order['orderId']} didn't go through. Returned by Binance: {processed_order}"
                    if self.telegram_logs is not None:
                        self.telegram_logs.sendMsg(message)
                    logger.info(message)

                    return {}

                else:
                    sleep(2)

    def placeTrailingStopLoss(self, symbol, side, quantity, activation_price, callback_rate):
        logger.info('Placing trailing stop-loss to {} {} {}'.format(side, quantity, symbol))
        self.testConnection()
        placed_tsl = {'symbol': None, 'ORDER_ID': None, 'QUANTITY': None, 'ACTIVATION_PRICE': None, 'CALLBACK_RATE': None}
        # Place trailing stop-loss
        try:
            # positionSide must be specified in hedge mode ('LONG' or 'SHORT')
            sent_tsl = self.client.futures_create_order(symbol=symbol, side=side, type=TRAILING_STOP_MARKET,
                                                        timeInForce=GTC, quantity=quantity,
                                                        activationPrice=activation_price, callbackRate=callback_rate,
                                                        workingType=LAST_TRADE_PRICE)
        except Exception as e:
            if self.telegram_logs is not None:
                self.telegram_logs.sendMsg('Exception: {}'.format(str(e)))
            logger.error('Exception: {}'.format(str(e)))
            return {}

        # Make sure order is placed properly
        # Initialize failed attempts counter
        i = 0
        while True:
            tsl = self.client.futures_get_order(symbol=symbol, orderId=sent_tsl['orderId'])
            if tsl['status'] == STATUS_NEW:
                placed_tsl = {'symbol': symbol, 'ORDER_ID': tsl['orderId'], 'QUANTITY': tsl['origQty'],
                              'ACTIVATION_PRICE': tsl['activatePrice'], 'CALLBACK_RATE': tsl['priceRate']}
                message = 'TSL {} has been placed. It will activate when {} price reaches {} and it will trail ' \
                          'the price with a callback rate of {}%'.format(placed_tsl['ORDER_ID'], symbol, placed_tsl['ACTIVATION_PRICE'],
                                                                         placed_tsl['CALLBACK_RATE'])
                self.telegram_logs.sendMsg(message)
                logger.info(message)

                return placed_tsl

            else:
                i += 1
                if i > 4:
                    message = "TSL didn't properly go through and is showing status {}".format(tsl['status'])
                    self.telegram_logs.sendMsg(message)
                    logger.error(message)

                    return {}

                sleep(2)

    def placeStopLoss(self, symbol, side, quantity, stop_price):
        logger.info('Placing stop-loss to {} {} {}'.format(side, quantity, symbol))
        self.testConnection()
        placed_sl = {'symbol': None, 'ORDER_ID': None, 'QUANTITY': None, 'STOP_PRICE': None}
        # Place stop-loss
        try:
            # positionSide must be specified in hedge mode ('LONG' or 'SHORT')
            sent_sl = self.client.futures_create_order(symbol=symbol, side=side, timeInForce=GTC,
                                                       type=STOP_MARKET, quantity=quantity, stopPrice=stop_price,
                                                       workingType=LAST_TRADE_PRICE)
        except Exception as e:
            if self.telegram_logs is not None:
                self.telegram_logs.sendMsg('Exception: {}'.format(str(e)))
            logger.info('Exception: {}'.format(str(e)))
            return {}

        # Make sure order is placed properly
        # Initialize failed attempts counter
        i = 0
        while True:
            sl = self.client.futures_get_order(symbol=symbol, orderId=sent_sl['orderId'])
            if sl['status'] == STATUS_NEW:
                placed_sl = {'symbol': symbol, 'ORDER_ID': sl['orderId'], 'QUANTITY': sl['origQty'],
                             'STOP_PRICE': sl['stopPrice']}
                message = 'Stop-loss {} has been placed with a stop price of {}'.format(placed_sl['ORDER_ID'], placed_sl['STOP_PRICE'])
                self.telegram_logs.sendMsg(message)
                logger.info(message)
                return placed_sl
            else:
                i += 1
                if i > 4:
                    message = "Stop-loss didn't properly go through and is showing status {}".format(sl['status'])
                    self.telegram_logs.sendMsg(message)
                    logger.error(message)

                    return {}

                sleep(2)

    def cancelOrder(self, symbol, order_id):
        self.testConnection()
        self.client.futures_cancel_order(symbol=symbol, orderId=order_id)
        # Initialize failed attempts counter
        i = 0
        while True:
            order = self.client.futures_get_order(symbol=symbol, orderId=order_id)
            if order['status'] == STATUS_CANCELED:
                message = 'Order {} was canceled'.format(order_id)
                if self.telegram_logs is not None:
                    self.telegram_logs.sendMsg(message)
                logger.info(message)

                return 1

            else:
                i += 1
                if i > 4:
                    message = "Order {} couldn't get canceled".format(order_id)
                    self.telegram_logs.sendMsg(message)
                    logger.error(message)

                    return -1

                sleep(2)
