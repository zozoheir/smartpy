from scipy.stats import pearsonr
import matplotlib.pyplot as plt
import seaborn as sns

def corrfunc(x, y, ax=None, **kws):
    """Plot the correlation coefficient in the top left hand corner of a plot."""
    r, _ = pearsonr(x, y)
    ax = ax or plt.gca()
    ax.annotate(f'ρ = {r:.2f}', xy=(.1, .9), xycoords=ax.transAxes)

def getPairPlotWithCorr(df):
    ax = sns.pairplot(df)
    ax.map_lower(corrfunc)
    return ax
