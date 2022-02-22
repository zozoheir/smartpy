import numpy as np
import pandas as pd
from hurst import compute_Hc
from typing import Union


def addRollingHurstExponent(stationary_series, jump_interval, lookback):
    """
    Get a list of rolling hurst exponent to measure mean reversion. No function found online
    """
    rolling_hurst = [float('nan') for i in range(lookback)]
    for i in range(lookback, len(stationary_series), jump_interval):
        series = stationary_series[i - lookback:i]
        hurst_exponent = compute_Hc(series)[0]
        data_points_left_to_compute = len(stationary_series) - i
        [rolling_hurst.append(hurst_exponent) for k in range(min(data_points_left_to_compute, jump_interval))]
    return rolling_hurst


def addRangeBreaks(series, range_lookback):
    """
    Another measure of mean reversion: % of time timeseries broke its range
    """
    df = pd.DataFrame(data=series, columns=['close'])
    df['rolling_max'] = df['close'].rolling(range_lookback).max()
    df['rolling_min'] = df['close'].rolling(range_lookback).min()
    df['break'] = np.where((df['close'] == df['rolling_min']) | (df['close'] == df['rolling_min']), 1, 0)
    df['break'] = df['break'].rolling(range_lookback).sum() / range_lookback
    df['break'] = df['break'].fillna(method='bfill')
    return np.array(df['break'])


def addZscore(df, column, lookbacks: Union[str, list]):
    if isinstance(column, str):
        column = [column]
    for column in column:
        if isinstance(lookbacks, int):
            lookbacks = [lookbacks]
        for i in lookbacks:
            df[f'{column}_zscore_{i}'] = (df[column] - df[column].rolling(i).mean()) / df[column].rolling(
                i).std()
    return df


def addMADifferences(df, column, lookbacks: list):
    ref_lookback = min(lookbacks)
    lookbacks = sorted(lookbacks)
    for i in lookbacks[1:]:
        df[f'{column}_ma_diff_{ref_lookback}m{i}'] = df[column].rolling(ref_lookback).mean() - df[column].rolling(
            i).mean()
    return df


def addMAs(df, column, lookbacks: list):
    for i in lookbacks:
        df[f'{column}_ma_{i}'] = df[column].rolling(i).mean()
    return df


def addPastForwardValues(df, target_column, lags: list, prefix=""):
    """
    Returns market data with past and forward returns
    """
    if prefix != "":
        prefix = prefix + "_"
    for t in lags:
        if t > 0:
            df[f'{prefix}t+{abs(t)}'] = df[target_column].shift(-abs(t))
        else:
            df[f'{prefix}t-{abs(t)}'] = df[target_column].shift(abs(t))
    return df


def addMarketImpacts(events_df=None,
                     market_data_df=None,
                     symbol_col=None,
                     time_column='timestamp',
                     timeframes=None,
                     interval=None,
                     z_score_lookback: int = None,
                     provider=None,
                     check_local_dir=True):
    # Add past timeframes to analyze pre post
    if min(timeframes) >= 0:
        timeframes = timeframes + [-30, -20, -10]

    df = pd.DataFrame({'timestamp': [dt.datetime.now()]})
    df['timestamp'] = pd.to_datetime(df.timestamp).dt.tz_localize('UTC')
    assert input_df[time_column].dtype == df.timestamp.dtypes, \
        "Market impact input_df time column should be a datetime64[ns, UTC] type"

    final_df = pd.DataFrame()
    # We get market impacts by symbol
    for sym in input_df[symbol_col].unique():
        rows_to_merge = input_df[input_df[symbol_col] == sym]

        price_data = self.getLast(provider, sym, interval, check_local_dir)
        past_forward_data = addPastForwardValues(df=price_data, target_column=sym, lags=timeframes)
        if z_score_lookback:
            past_forward_data = addZscore(past_forward_data, column=sym, mean_lookback=z_score_lookback,
                                          std_lookback=z_score_lookback)
            sym_zscore = sym + f'_zscore+{z_score_lookback}'
            for timeframe_col in [i for i in past_forward_data.columns if 't+' in i or 't-' in i]:
                past_forward_data[timeframe_col] = past_forward_data[timeframe_col] / past_forward_data[
                    sym_zscore] - 1
        else:
            for timeframe_col in [i for i in past_forward_data.columns if 't+' in i or 't-' in i]:
                past_forward_data[timeframe_col] = past_forward_data[timeframe_col] / past_forward_data[sym] - 1
        past_forward_data = past_forward_data.drop(sym, axis=1)

        rows_to_merge = rows_to_merge.sort_values('timestamp')
        past_forward_data = past_forward_data.sort_values('timestamp')
        tmp = pd.merge_asof(rows_to_merge, past_forward_data)
        final_df = final_df.append(tmp)

    return final_df.sort_values(by=time_column)
