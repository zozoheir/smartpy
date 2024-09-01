from skopt import gp_minimize
from skopt.space import Integer, Real
from skopt.utils import use_named_args
import numpy as np

from scripts.state_strategy import TradingStrategy

space = [
    Integer(1, 12, name='lookback_months'),
    Integer(2, 5, name='n_bins'),
    Real(0, 0.03, name='total_pnl_threshold'),
    Real(0.3, 0.7, name='stability_threshold')
]


runs =  []
@use_named_args(space)
def objective(**params):
    # Create and run the strategy with the provided parameters
    strategy = TradingStrategy(
        lookback_months=params['lookback_months'],
        start_date='2010-01-01',
        end_date='2018-01-01',
        retraining_frequency='weekends',
        tradeable='SPY',
        state_indicators=['^VIX', 'GLD', 'SPY', '^TNX'],
        total_pnl_threshold=params['total_pnl_threshold'],
        n_bins=params['n_bins'],
        stability_threshold=params['stability_threshold'],
        discretization_method='qcut'
    )

    strategy.simulate_trading()

    # Print params and results
    print(f"Params: {params}")
    print(f"----RESULT")
    print(f"Total PnL: {np.sum(strategy.pnl)}")
    print(f"Trades: {len(strategy.trades)}")
    print(f"Sharpe Ratio: {np.mean(strategy.pnl) / np.std(strategy.pnl) * np.sqrt(252)}")
    if len(strategy.pnl) > 0:
        print(f"Win Rate: {len([pnl for pnl in strategy.pnl if pnl > 0]) / len(strategy.pnl)}")

    final_pnl = -np.sum(strategy.pnl)
    runs.append({
        'params':params,
        'strategy':strategy
    })

    return final_pnl




def run_opt():
    results = gp_minimize(
        func=objective,
        dimensions=space,
        n_calls=20,
        random_state=42,
        n_initial_points=10,
        n_jobs=-1
    )
    best_params = results.x
    print("Best Parameters:", best_params)
    print("Best Objective Value:", -results.fun)


    # Save runs as pickle


run_opt()


