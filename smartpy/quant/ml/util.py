import smartpy.utility.dt_util as dt_util
import pandas as pd
import datetime as dt
import numpy as np

def getTrainTestData(df, trading_date, training_size_days, X_features, Y_feature, up_class_cutoff=0):
    # Get last training date T-1
    train_end_date = dt_util.toDatetime(trading_date) - dt.timedelta(days=1)
    train_end_date = pd.Timestamp(train_end_date)
    # Expanding or rolling window
    if training_size_days is None:
        train_df = df[df['timestamp'].dt.date <= train_end_date]
    else:
        train_start_date = dt_util.toDatetime(trading_date) - dt.timedelta(days=training_size_days)
        train_start_date = pd.Timestamp(train_start_date)
        train_df = df[(df['timestamp'].dt.date >= train_start_date) & (df['timestamp'].dt.date <= train_end_date)]
    test_df = df[df['timestamp'].dt.date == pd.Timestamp(trading_date)]

    X_train = train_df[X_features]
    X_test = test_df[X_features]

    Y_train_class = np.where(train_df[Y_feature] > up_class_cutoff, 1, 0)
    Y_train_reg = train_df[Y_feature]
    Y_test_class = np.where(test_df[Y_feature] > up_class_cutoff, 1, 0)
    Y_test_reg = test_df[Y_feature]

    model_data = {'train_df': train_df.reset_index(drop=True),
                  'test_df': test_df.reset_index(drop=True),
                  'X_train': X_train.reset_index(drop=True),
                  'X_test': X_test.reset_index(drop=True),
                  'Y_train_class': Y_train_class,
                  'Y_test_class': Y_test_class,
                  'Y_train_reg': Y_train_reg,
                  'Y_test_reg': Y_test_reg,
                  }

    return model_data


def scalerFitTransform(df, scaler):
    #reshaped = np.array(open_orders).reshape(1,-1)
    rescaled_data = scaler.fit_transform(df)
    return scaler, pd.DataFrame(rescaled_data, columns=df.columns)

def scalerTransform(df, scaler):
    #reshaped = np.array(open_orders).reshape(1,-1)
    rescaled_data = scaler.fit_transform(df)
    return pd.DataFrame(rescaled_data, columns=df.columns)

