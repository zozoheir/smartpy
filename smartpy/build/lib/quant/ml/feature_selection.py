import random
import warnings

import numpy as np
import pandas as pd
import scipy
from sklearn.decomposition import PCA
from sklearn.linear_model import LogisticRegression, LinearRegression
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn.tree import DecisionTreeClassifier, DecisionTreeRegressor

warnings.filterwarnings("ignore")

std_scaler = StandardScaler()
minmax_scaler = MinMaxScaler()


class FeatureSelector:

    def __init__(self, X: pd.DataFrame, Y: pd.DataFrame, permutation_model=None, decision_tree_n=5):
        self.X = X
        self.Y = Y
        self.features = X.columns
        self.best_features = self.features
        self.percentage_kept = 0
        self.feature_importances = pd.DataFrame({'feature': self.features})
        self.permutation_model = permutation_model
        self.decision_tree_n = decision_tree_n

    def applyDecisionTree(self, decision_tree_model, param_grid, decision_tree_filter_n_runs):
        if decision_tree_filter_n_runs > 0:
            features = pd.DataFrame({'feature': self.features})
            for i in range(decision_tree_filter_n_runs):
                params = {key: random.choice(val) for key, val in param_grid.items()}
                random_model = decision_tree_model(**params)
                random_model.fit(self.X, self.Y)
                features[f"trial_{i}"] = random_model.feature_importances_
            features['average_importance'] = features.mean(axis=1)
            features_to_keep = list(features[features.average_importance > 1 / len(self.X.columns)]['feature'])
            self.tree_importances = features

            self._addFeatures(features_to_keep)
            self.feature_importance_decision_tree = features
            self.feature_importances['decision_tree'] = minmax_scaler.fit_transform(
                reshapeForScaler(features['average_importance']))
            self._standardizeImportances()

    def applyCorrelationFilter(self, correlation_filter_cutoff=0):
        corrs = {i: [] for i in ['feature', 'pearson', 'spearman']}
        for feature in self.features:
            corrs['feature'].append(feature)
            corrs['pearson'].append(abs(scipy.stats.pearsonr(self.Y, self.X[feature])[0]))
            corrs['spearman'].append(abs(scipy.stats.spearmanr(self.Y, self.X[feature])[0]))
        features_correls_with_target_df = pd.DataFrame(corrs)
        features_correls_with_target_df['max_corr'] = features_correls_with_target_df[['pearson', 'spearman']].max(axis=1)
        features_to_keep = list(features_correls_with_target_df[features_correls_with_target_df.max_corr >= correlation_filter_cutoff]['feature'])
        self.correlation_importances = features_correls_with_target_df

        self._addFeatures(features_to_keep)
        self.feature_importances['correlations'] = minmax_scaler.fit_transform(
            reshapeForScaler(features_correls_with_target_df['max_corr']))
        self._standardizeImportances()

    def removeCollinearFeatures(self, multicollinearity_filter_cutoff):

        # Rank best features by importance and get their correlation matrix
        best_features = list(self.best_features)
        all_feature_importances = self.feature_importances
        best_feature_importances = all_feature_importances[all_feature_importances.feature.isin(best_features)]
        best_features = list(best_feature_importances['feature'])
        best_features_corr = self.X[best_features].corr()

        # Get multicollinear pairs
        high_corr_pairs = []
        best_features_correl = best_features_corr[best_features_corr.index.isin(best_features)]
        for feature in best_features_correl.columns:
            feature_corrs = best_features_correl[feature].drop(feature)
            for multicollin_feature in feature_corrs.index:
                correl = feature_corrs[multicollin_feature]
                if correl >= multicollinearity_filter_cutoff:
                    high_corr_pairs.append((feature, multicollin_feature))

        # Keep dropping until no more to drop
        dropped = []
        i = 0
        while i != len(best_features):
            to_analyze = best_features[i]
            for pair in high_corr_pairs:
                if pair[0] == to_analyze:
                    todrop = pair[1]
                    if todrop in best_features:
                        best_features.remove(todrop)
                        dropped.append(todrop)
            i += 1
        self.best_features = best_features
        self.percentage_kept = len(self.best_features) / len(self.features)
        self.dropped_multicollinear_features = dropped

    def applyFeatureSelection(self,
                              correlation_filter_cutoff,
                              decision_tree_filter_n_runs,
                              multicollinearity_filter_cutoff):
        self.applyCorrelationFilter(correlation_filter_cutoff)
        self.applyDecisionTree(decision_tree_filter_n_runs)
        self.removeCollinearFeatures(multicollinearity_filter_cutoff)

    def applyPCA(self):
        pca = PCA(n_components=100)
        pca.fit(std_scaler.fit_transform(self.X))
        explained_vars = pca.explained_variance_ratio_ * 100
        features_entropy_disorder = scipy.stats.entropy(explained_vars) / scipy.stats.entropy(
            [1 / 100 for i in range(100)])
        self.features_entropy_disorder = features_entropy_disorder
        self.pca_explained_vars = explained_vars

    def _addFeatures(self, features_to_keep):
        self.best_features = [i for i in self.best_features if i in features_to_keep]
        self.percentage_kept = len(self.best_features) / len(self.features)

    def _standardizeImportances(self):
        scores_columns = [i for i in self.feature_importances.columns if 'aggregate_importance' not in i]
        self.feature_importances['aggregate_importance'] = self.feature_importances[scores_columns].mean(axis=1)
        self.feature_importances = self.feature_importances.sort_values(by='aggregate_importance', ascending=False)
        self.feature_importances = self.feature_importances.dropna().reset_index(drop=True)


class FeatureSelectorRegression(FeatureSelector):

    def __init__(self, X, Y, permutation_model=LinearRegression(), decision_tree_n=5):
        super().__init__(X, Y, permutation_model=permutation_model, decision_tree_n=decision_tree_n)

    def applyDecisionTree(self, decision_tree_filter_n_runs=0):
        param_grid = {
            "splitter": ['best'],
            "max_features": range(1, len(self.features)),
            "criterion": ['mse', 'friedman_mse', 'mae'],
            "max_depth": list(range(1, len(self.features) * 2)),
            "min_samples_split": list(range(int(0.05 * len(self.X)), int(0.5 * len(self.X)))),
        }
        super().applyDecisionTree(DecisionTreeRegressor, param_grid, decision_tree_filter_n_runs)


class FeatureSelectorClassification(FeatureSelector):

    def __init__(self, X, Y, permutation_model=LinearRegression(), decision_tree_n=5):
        super().__init__(X, Y, permutation_model=permutation_model, decision_tree_n=decision_tree_n)

    def applyDecisionTree(self, decision_tree_filter_n_runs=5):
        decision_tree_grid = {
            "splitter": ['best'],
            "criterion": ['gini', 'entropy'],
            "max_depth": list(range(1, len(self.features) * 2)),
            "min_samples_split": list(range(1, int(0.1 * len(self.X)))),
            "class_weight": 'balanced'
        }
        super().applyDecisionTree(DecisionTreeClassifier, decision_tree_grid, decision_tree_filter_n_runs)


def reshapeForScaler(df):
    return np.array(df).reshape(-1, 1)
