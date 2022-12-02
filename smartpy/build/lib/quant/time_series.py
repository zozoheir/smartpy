import numpy as np
import pandas as pd
from typing import Union
import datetime as dt

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


def addEventImpact(events_df,
                   market_data_df,
                   events_time_column='timestamp',
                   market_data_price_column='close',
                   market_data_time_column='timestamp',
                   snapshots_intervals=None,
                   ):
    # Add past snapshots_intervals to analyze pre post
    if min(snapshots_intervals) >= 0:
        snapshots_intervals = snapshots_intervals + [-30, -20, -10]

    # Sort by timestamp for merge asof
    events_df = events_df.sort_values(by=events_time_column)
    market_data_df = market_data_df.sort_values(by=market_data_time_column)

    # Checking that formats are the same - formatting coding shortcut...
    df = pd.DataFrame({'timestamp': [dt.datetime.now()]})
    df['timestamp'] = pd.to_datetime(df.timestamp).dt.tz_localize('UTC')
    assert events_df[events_time_column].dtype == df.timestamp.dtypes, \
        "Market impact events_df time column should be a datetime64[ns, UTC] types"

    past_forward_data = addPastForwardValues(df=market_data_df, target_column=market_data_price_column,
                                             lags=snapshots_intervals)
    past_forward_columns = [i for i in past_forward_data.columns if 't+' in i or 't-' in i]
    for col in past_forward_columns:
        past_forward_data[col] = past_forward_data[col] / past_forward_data['close'] - 1

    final_df = pd.merge_asof(events_df, past_forward_data, left_on=events_time_column, right_on=market_data_time_column)

    return final_df
