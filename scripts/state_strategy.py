import numpy as np
import plotly.graph_objects as go
import warnings
from logging import getLogger

import matplotlib
import matplotlib.pyplot as plt
import pandas as pd
import yfinance as yf
from scipy.spatial.distance import cdist

warnings.filterwarnings('ignore')

matplotlib.use('TkAgg')

logger = getLogger(__name__)


class TradingStrategy:
    def __init__(self,
                 lookback_months,
                 start_date,
                 end_date,
                 retraining_frequency,
                 tradeable,
                 state_indicators,
                 sharpe_cutoff=1.5,
                 total_pnl_threshold=0.1,
                 stability_threshold=0.6,
                 n_bins=2,
                 discretization_method='qcut'):
        self.lookback_months = lookback_months
        self.start_date = pd.to_datetime(start_date)
        self.end_date = pd.to_datetime(end_date)
        self.retraining_frequency = retraining_frequency
        self.tradeable = tradeable
        self.state_indicators = state_indicators
        self.n_bins = n_bins
        self.trades = []  # Track trades
        self.pnl = []  # Track daily pnl
        self.last_training_date = None
        self.buy_and_hold_pnl = []  # Track PnL for buy-and-hold
        self.profitable_states_long = []
        self.profitable_states_short = []
        self.sharpe_cutoff = sharpe_cutoff
        self.stability_threshold = stability_threshold
        self.discretization_method = discretization_method
        self.total_pnl_threshold = total_pnl_threshold

    def discretize(self,
                   data):
        if self.discretization_method == 'qcut':
            # Discretize indicators and store discretization stats
            for ticker in self.state_indicators:
                data[f'{ticker}_Discrete'] = pd.qcut(data[ticker].rank(method='first'), self.n_bins, labels=False) / (
                        self.n_bins - 1)
            data['state'] = data[[f'{ticker}_Discrete' for ticker in self.state_indicators]].round(2).apply(
                lambda row: tuple(row.values), axis=1)
            return data

    def fetch_data(self):
        # Fetch data from Yahoo Finance
        tickers = [self.tradeable] + self.state_indicators
        data = \
            yf.download(tickers, start=self.start_date - pd.DateOffset(months=self.lookback_months), end=self.end_date)[
                'Adj Close'].ffill().dropna()
        data[f'{self.tradeable}_Return'] = data[self.tradeable].pct_change().shift(-1)

        for indicator in self.state_indicators:
            data[f'{indicator}_diff'] = data[indicator].pct_change()

        self.state_indicators = [f'{indicator}_diff' for indicator in self.state_indicators] + self.state_indicators
        data.index = data.index.tz_localize(None)
        return data

    def evaluate_state_stability(self,
                                 profitable_state,
                                 data,
                                 n=10,
                                 side='long'):
        state_matrix = data['state'].unique().tolist()
        distances = cdist([profitable_state], state_matrix, metric='euclidean')[0]
        similar_state_indices = distances.argsort()[1:n + 1]
        similar_states = [state_matrix[i] for i in similar_state_indices]
        similar_state_data = data[data['state'].isin(similar_states)]
        if side == 'long':
            pnl_by_state = similar_state_data.groupby('state').sum()[f'{self.tradeable}_Return']
        else:
            pnl_by_state = -similar_state_data.groupby('state').sum()[f'{self.tradeable}_Return']

        pct_profitable_states = len(pnl_by_state[pnl_by_state > 0]) / len(pnl_by_state)
        if pct_profitable_states >= self.stability_threshold:
            return True
        else:
            return False

    def find_profitable_states_long(self, data):
        # Create market state
        data = self.discretize(data)

        # Group by state and calculate mean returns
        state_returns = data.groupby('state')[f'{self.tradeable}_Return'].agg(['mean', 'count', 'sum', 'std'])

        state_returns['sharpe'] = state_returns['mean'] / state_returns['std']
        state_returns = state_returns.sort_values(by='sharpe', ascending=False)
        # Calculate win (positive returns) and loss (negative returns) counts
        state_wins = data.groupby('state')[f'{self.tradeable}_Return'].apply(lambda x: (x > 0).sum())
        state_losses = data.groupby('state')[f'{self.tradeable}_Return'].apply(lambda x: (x < 0).sum())

        # Add win/loss columns to the state_returns dataframe
        state_returns['wins'] = state_wins
        state_returns['losses'] = state_losses

        # Calculate win/loss ratio
        state_returns['win_loss_ratio'] = state_returns['wins'] / (state_returns['losses'] + state_returns['wins'])

        # Identify profitable states with significant positive returns
        # good_states = state_returns[(state_returns['count'] > 2) &
        #                            (state_returns['sharpe'] > self.sharpe_cutoff) &
        #                            (state_returns['win_loss_ratio'] >= 0.6)].index

        good_states = state_returns[state_returns['sum'] > self.total_pnl_threshold].index

        picked_states = []
        for profitable_state in good_states:
            is_stable = self.evaluate_state_stability(profitable_state, data)
            if is_stable:
                picked_states.append(profitable_state)

        return picked_states

    def find_profitable_states_short(self, data):
        # Create market state
        data = self.discretize(data)
        data[f'{self.tradeable}_Return'] = -data[f'{self.tradeable}_Return']
        # Group by state and calculate mean returns
        state_returns = data.groupby('state')[f'{self.tradeable}_Return'].agg(['mean', 'count', 'sum', 'std'])

        state_returns['sharpe'] = state_returns['mean'] / state_returns['std']
        state_returns = state_returns.sort_values(by='sharpe', ascending=False)

        state_wins = data.groupby('state')[f'{self.tradeable}_Return'].apply(lambda x: (x > 0).sum())
        state_losses = data.groupby('state')[f'{self.tradeable}_Return'].apply(lambda x: (x < 0).sum())

        # Add win/loss columns to the state_returns dataframe
        state_returns['wins'] = state_wins
        state_returns['losses'] = state_losses

        # Calculate win/loss ratio
        state_returns['win_loss_ratio'] = state_returns['wins'] / (state_returns['losses'] + state_returns['wins'])

        # Identify profitable states with significant positive returns
        # good_states = state_returns[(state_returns['count'] > 1) &
        #                            (state_returns['sharpe'] >= self.sharpe_cutoff) &
        #                            (state_returns['win_loss_ratio'] >= 0.5)].index

        good_states = state_returns[state_returns['sum'] > self.total_pnl_threshold].index

        picked_states = []
        for profitable_state in good_states:
            is_stable = self.evaluate_state_stability(profitable_state, data, side='short')
            if is_stable:
                picked_states.append(profitable_state)

        return picked_states

    def simulate_trading(self):
        """
        Simulates the trading strategy over the test period.
        """
        # Fetch full data from start to end date
        data = self.fetch_data()
        data = data.dropna()

        # Calculate Buy-and-Hold PnL
        self.buy_and_hold_pnl = (data[f'{self.tradeable}_Return']).cumsum()

        current_date = self.start_date
        while current_date <= self.end_date:

            # Retrain on weekends (Saturday)
            if self.last_training_date is None or current_date.weekday() == 5:
                lookback_end = current_date
                lookback_start = current_date - pd.DateOffset(months=self.lookback_months)
                train_data = data.loc[lookback_start:lookback_end].copy()
                self.profitable_states_long = self.find_profitable_states_long(train_data)
                self.profitable_states_short = self.find_profitable_states_short(train_data)
                self.last_training_date = current_date

            # Trading Logic: Check if today is in a profitable state
            if current_date in data.index and current_date.weekday() < 5:  # Trade only on weekdays
                # Discretize current values using training statistics
                discretization_data = data.loc[lookback_start:current_date]
                discretization_data = self.discretize(discretization_data)

                current_state = discretization_data.loc[current_date, 'state']
                long_signal = 1 if current_state in self.profitable_states_long else 0
                short_signal = 1 if current_state in self.profitable_states_short else 0

                # If signal is 1, we buy at close and sell next day's close
                if long_signal:
                    entry_price = data.loc[current_date, self.tradeable]
                    exit_date = data[data.index > current_date]
                    if len(exit_date) == 0:
                        break
                    else:
                        exit_date = exit_date.index[0]
                    exit_price = data.loc[exit_date, self.tradeable]
                    pnl = (exit_price - entry_price) / entry_price
                    self.trades.append({
                        'entry_date': current_date,
                        'exit_date': exit_date,
                        'entry_price': entry_price,
                        'exit_price': exit_price,
                        'pnl': pnl
                    })
                    self.pnl.append(pnl)

                elif short_signal:
                    entry_price = data.loc[current_date, self.tradeable]
                    exit_date = data[data.index > current_date]
                    if len(exit_date) == 0:
                        break
                    else:
                        exit_date = exit_date.index[0]

                    exit_price = data.loc[exit_date, self.tradeable]
                    pnl = (entry_price - exit_price) / entry_price
                    self.trades.append({
                        'entry_date': current_date,
                        'exit_date': exit_date,
                        'entry_price': entry_price,
                        'exit_price': exit_price,
                        'pnl': pnl
                    })
                    self.pnl.append(pnl)

            current_date += pd.Timedelta(days=1)

    def plot_cumulative_pnl(self):
        """
        Plots the cumulative PnL of the trading strategy along with buy-and-hold PnL using Plotly.
        """
        # Calculate cumulative PnL of the strategy
        cumulative_pnl = np.cumsum(self.pnl)
        dates = [trade['exit_date'] for trade in self.trades]

        # Create Plotly figure

        fig = go.Figure()

        # Add the strategy cumulative PnL line
        fig.add_trace(go.Scatter(
            x=dates,
            y=cumulative_pnl,
            mode='lines',
            name='Strategy Cumulative PnL'
        ))

        # Add the buy-and-hold cumulative PnL line
        fig.add_trace(go.Scatter(
            x=self.buy_and_hold_pnl.index,
            y=self.buy_and_hold_pnl.values,
            mode='lines',
            name='Buy and Hold Cumulative PnL',
            line=dict(dash='dash')
        ))

        # Update the layout of the figure
        fig.update_layout(
            title='Cumulative PnL of Trading Strategy vs. Buy and Hold',
            xaxis_title='Date',
            yaxis_title='Cumulative Returns',
            legend=dict(x=0, y=1),
            xaxis=dict(showgrid=True),
            yaxis=dict(showgrid=True),
            template='plotly_white'
        )

        # Show the figure
        fig.show()
