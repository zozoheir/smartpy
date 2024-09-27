import numpy as np
import pandas as pd


def getMLStrategyReturns(trading_data, target_column):
    pred_cutoff = 0.5
    pnl = position = 0
    data = {i: [] for i in ['timestamp', 'position', 'y_pred_proba', 'sym_r_pct', 'pnl_pct']}
    for i in range(len(trading_data)):
        timestamp = trading_data['timestamp'].iloc[i]
        y_pred_proba = trading_data['y_pred_proba'].iloc[i]
        target = trading_data[target_column].iloc[i]

        if position == 0:
            if y_pred_proba > pred_cutoff:
                pnl = target - 7 / 10000
                position = 1
            elif y_pred_proba < pred_cutoff:
                pnl = -target - 7 / 10000
                position = -1
            else :
                pnl = 0
        elif position == -1:
            if y_pred_proba > pred_cutoff:
                pnl = target - 2 * 7 / 10000
                position = 1
            elif y_pred_proba < pred_cutoff:
                pnl = -target
            else :
                pnl = -7/10000
        elif position == 1:
            if y_pred_proba > pred_cutoff:
                pnl = target
            elif y_pred_proba < pred_cutoff:
                pnl = -target - 2 * 7 / 10000
                position = -1
            else :
                pnl = -7/10000

        data['timestamp'].append(timestamp)
        data['y_pred_proba'].append(y_pred_proba)
        data['position'].append(position)
        data['pnl_pct'].append(pnl)
        data['sym_r_pct'].append(target)

    df = pd.DataFrame(data)
    df['pnl_cumul'] = df['pnl_pct'].cumsum()
    return df


def bootstrap_samples(start_date, end_date, num_samples=100, pct_samples=None):
    n_days = int(pct_samples * len(pd.date_range(start_date, end_date)))
    max_start_date = pd.to_datetime(end_date) - pd.Timedelta(days=n_days - 1)
    start_dates = pd.to_datetime(np.random.choice(pd.date_range(start_date, max_start_date), size=num_samples))
    end_dates = start_dates + pd.Timedelta(days=n_days - 1)
    return list(zip(start_dates, end_dates))