import requests
import hashlib
import hmac
import concurrent
from concurrent.futures import ThreadPoolExecutor
from ccxt.base.errors import ExchangeNotAvailable, RequestTimeout, NetworkError, ExchangeError
from urllib3.exceptions import ProtocolError
from socket import timeout
from requests.exceptions import HTTPError
from sqlalchemy.pool.impl import QueuePool


import smartpy.utility.dt_util as dt_util

CCXT_EXCEPTIONS = [QueuePool,
                   HTTPError,
                   TimeoutError,
                   timeout,
                   ConnectionResetError,
                   ProtocolError,
                   NetworkError,
                   ExchangeNotAvailable,
                   RequestTimeout,
                   ExchangeError]


TIMEFRAMES = ['10min', '30min']

host = "https://api.gateio.ws"
prefix = "/api/v4"
headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
url = '/spot/orders'


def gen_sign(method, url, query_string=None, payload_string=None):
    key = MY_API_KEYS[GATEIO_CCXT]['apiKey']  # api_key
    secret = MY_API_KEYS[GATEIO_CCXT]['secret']  # api_secret

    t = time.time()
    m = hashlib.sha512()
    m.update((payload_string or "").encode('utf-8'))
    hashed_payload = m.hexdigest()
    s = '%s\n%s\n%s\n%s\n%s' % (method, url, query_string or "", hashed_payload, t)
    sign = hmac.new(secret.encode('utf-8'), s.encode('utf-8'), hashlib.sha512).hexdigest()
    return {'KEY': key, 'Timestamp': str(t), 'SIGN': sign}


def requestGateioTradesText(symbol):
    symbol = symbol.replace('/', '_')

    timestamp_intervals = dt_util.getPaginationIntervals(start='2021-12-01',
                                                         end=dt.datetime.now(),
                                                         freq='7d')

    orders_df = pd.DataFrame()

    for interval in timestamp_intervals:
        page = 1
        # interval = timestamp_intervals[3]
        while True:
            query_param = f'currency_pair={symbol}&status=finished&page={page}&limit=100&title={str(interval[0])}&to={str(interval[1])}'
            sign_headers = gen_sign('GET', prefix + url, query_param)
            headers.update(sign_headers)
            r = requests.request('GET', host + prefix + url + "?" + query_param, headers=headers)
            if r.status_code == 200:
                results = pd.DataFrame(r.json())
                if len(results) > 0:
                    orders_df = orders_df.append(results[['id', 'text', 'currency_pair']])
                else:
                    break
                page += 1
            else:
                raise GateIOAPIRequestError(f"Gate Function API spot orders request issue : {r.content}")

        if len(orders_df) == 0 and page == 1:
            break
    return orders_df


def getAllGateIOTradesThreading(symbols):
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = []
        for symbol in symbols:
            futures.append(executor.submit(requestGateioTradesText, symbol))

    results = concurrent.futures.as_completed(futures)
    results = [i.result() for i in results]
    orders_df = pd.DataFrame()
    for result in results:
        orders_df = orders_df.append(result)
    return orders_df


def processGateIOCCXTOrdersDF(trades_df):
    to_process = trades_df.copy()
    to_process['datetime'] = to_process['datetime'].apply(lambda x: str(x).replace('Z', '').replace('T', ' '))
    to_process['datetime'] = pd.to_datetime(to_process['datetime'])
    to_process['fee_currency'] = to_process['fees'].apply(lambda x: getFeeAttribute(x, 'currency'))
    to_process['fee_amount'] = to_process['fees'].apply(lambda x: getFeeAttribute(x, 'cost'))
    to_process['fee_USDT'] = np.where(to_process['fee_currency'] == 'USDT',
                                      to_process['fee_amount'].astype(float),
                                      to_process['fee_amount'].astype(float) * to_process['price'])
    to_process.drop(['info', 'fee', 'fees', 'trades'], axis=1, inplace=True)
    to_process['coin'] = to_process['coin'].apply(lambda x: x.split('/')[0])
    return to_process


def getFeeAttribute(fee_list, attribute):
    for fee_dict in fee_list:
        if fee_dict['currency'] != 'GT':
            return fee_dict[attribute]


def downloadFTXTrades(coin, hours_lookback=24):
    one_hour = 86400000 / 24
    since = ftx_exchange.milliseconds() - one_hour*hours_lookback
    # alternatively, fetch from a certain starting datetime
    # since = ftx_exchange.parse8601('2018-01-01T00:00:00Z')
    all_orders = []
    while since < ftx_exchange.milliseconds():
        symbol = f'{coin}-PERP'  # change for your coin
        limit = 200000  # change for your limit
        orders = ftx_exchange.fetchTrades(symbol, since, limit)
        if len(orders):
            since = orders[len(orders) - 1]['timestamp'] + 1
            all_orders += orders
        else:
            break

    orders_df = pd.DataFrame([i['info'] for i in all_orders])
    if len(orders_df) > 0:
        orders_df[['size', 'price']] = orders_df[['size', 'price']].astype(float)
        orders_df['amount_usd'] = orders_df['size'] * orders_df['price']
        orders_df['signed_amount'] = np.where(orders_df['side'] == 'buy', orders_df['size'],
                                              -orders_df['size'])
        orders_df['signed_amount_usd'] = np.where(orders_df['side'] == 'buy', orders_df['amount_usd'],
                                                  -orders_df['amount_usd'])
        orders_df['timestamp'] = orders_df['time'].apply(lambda x: dt_util.toDatetime(x.replace('T', ' ')))
        orders_df['coin'] = coin
    return orders_df


