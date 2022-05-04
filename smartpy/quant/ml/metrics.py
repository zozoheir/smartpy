import numpy as np
import sklearn.metrics as metrics

def getRegressionMetrics(y_true, y_pred):
    """Returns a dictionary of standard regression metrics"""
    y_true = np.array(y_true)
    y_pred = np.array(y_pred)

    sst = np.sum((y_true-np.average(y_true))**2)
    sse = np.sum((y_true-y_pred)**2)
    n = len(y_true)

    # Regression stats
    beta = np.dot(y_pred, y_true) / np.dot(y_pred, y_pred)
    regression_stats = {
        "n": n,
        "sse":sse,
        "sst":sst,
        "r2": metrics.r2_score(y_true=y_true, y_pred=y_pred),
        "rmse": np.sqrt(metrics.mean_squared_error(y_true=y_true, y_pred=y_pred)),
        "mae": metrics.mean_absolute_error(y_true=y_true, y_pred=y_pred),
        "beta": beta,
        "r2_scaled": metrics.r2_score(y_true, y_pred * beta),
    }
    y_pred_class = np.where(y_pred > 0, 1, 0)
    y_true_class = np.where(y_true > 0, 1, 0)
    accuracy = metrics.accuracy_score(y_true_class, y_pred_class)
    percentage_upticks = np.average(np.where(y_true_class > 0, 1, 0))

    # Classification stats
    classification_report = metrics.classification_report(y_true_class, y_pred_class, output_dict=True)
    binary_class_benchmark = max(percentage_upticks, 1 - percentage_upticks)
    classification_stats = {
        "accuracy": accuracy,
        "auc_score": metrics.roc_auc_score(y_true_class, y_pred_class),
        "percentage_upticks": percentage_upticks,
        "accuracy_alpha": accuracy - binary_class_benchmark,
        "1_precision_alpha": classification_report['1']['precision'] - binary_class_benchmark,
        "1_recall_alpha": classification_report['1']['recall'] - binary_class_benchmark,
    }
    classification_stats.update(classification_report)
    regression_stats.update(classification_stats)
    return regression_stats


