
from sklearn.preprocessing import OneHotEncoder
import pandas as pd
import time
import sys
from src.func_model_tracker import *


def split_data(df, test_month, val_month=None):
    """
    Take an input df, test month and val month
    Return feature df and label series for training, val and test data sets, and a prediction df
    :param df: a dataframe with features and labels
    :param test_month: string, data period for testing
    :param val_month: string,  data period for validation
    :return: features df and label seires for training, val and test dataset, and prediction df
    """
    if val_month is None:
        if df.YEAR_MONTH.max() < test_month:
            sys.exit('Inappropriate period selection')
        else:
            X_train = df[df.YEAR_MONTH < test_month]
            X_test = df[df.YEAR_MONTH == test_month]

            # Create y
            y_train = X_train.BOOKED
            y_test = X_test.BOOKED

            # Create pred df
            df_pred = X_test[['ID', 'YEAR_MONTH', 'BOOKED']].rename(columns={'BOOKED': 'ACTUAL'})

            # drop BOOKED
            X_train = X_train.drop(['BOOKED'], axis=1)
            X_test = X_test.drop(['BOOKED'], axis=1)

            return X_train, X_test, y_train, y_test, df_pred

    else:
        if (test_month > val_month) & (df.YEAR_MONTH.max() >= test_month):
            # create X
            X_train = df[df.YEAR_MONTH < val_month]
            X_val = df[df.YEAR_MONTH == val_month]
            X_test = df[df.YEAR_MONTH == test_month]

            # Create y
            y_train = X_train.BOOKED
            y_val = X_val.BOOKED
            y_test = X_test.BOOKED

            # Create pred df
            df_pred = X_test[['ID', 'YEAR_MONTH', 'BOOKED']].rename(columns={'BOOKED': 'ACTUAL'})

            # drop BOOKED
            X_train = X_train.drop(['BOOKED'], axis=1)
            X_val = X_val.drop(['BOOKED'], axis=1)
            X_test = X_test.drop(['BOOKED'], axis=1)
            return X_train, X_val, X_test, y_train, y_val, y_test, df_pred

        else:
            sys.exit('Inappropriate period selection')


def encode_data(cols, df_train, df_test=None, df_val=None, out_encoder = False):
    """
    Encode categorical columns in train, val and test datasets
    :param df_train: training data
    :param df_val: val data
    :param df_test: test data
    :param cols: categorical columns
    :return:
    """

    ohe = OneHotEncoder(handle_unknown='ignore', sparse=False)
    df_ohe_train = pd.DataFrame(ohe.fit_transform(df_train[cols]))
    # df_ohe_val = pd.DataFrame(ohe.transform(df_val[cols]))
    # df_ohe_test = pd.DataFrame(ohe.transform(df_test[cols]))

    # One-hot encoding removed index, put it back
    df_ohe_train.index = df_train.index
    # df_ohe_val.index = df_val.index
    # df_ohe_test.index = df_test.index

    # Get the numerical columns
    df_train_num = df_train.drop(cols, axis=1)
    # df_val_num = df_val.drop(cols, axis=1)
    # df_test_num = df_test.drop(cols, axis=1)

    # Add one-hot encoded columns to numerical columns
    df_train_ohe = pd.concat([df_train_num, df_ohe_train], axis=1)
    # df_test_ohe = pd.concat([df_test_num, df_ohe_test], axis=1)
    # df_val_ohe = pd.concat([df_val_num, df_ohe_val], axis=1)

    if (df_test is None) & (df_val is None) & (out_encoder):
        return df_train_ohe, ohe

    else:
        df_ohe_val = pd.DataFrame(ohe.transform(df_val[cols]))
        df_ohe_test = pd.DataFrame(ohe.transform(df_test[cols]))

        df_ohe_val.index = df_val.index
        df_ohe_test.index = df_test.index

        df_val_num = df_val.drop(cols, axis=1)
        df_test_num = df_test.drop(cols, axis=1)

        df_val_ohe = pd.concat([df_val_num, df_ohe_val], axis=1)
        df_test_ohe = pd.concat([df_test_num, df_ohe_test], axis=1)
        return df_train_ohe, df_val_ohe, df_test_ohe


def train_and_eval_model(model, X_train, y_train, X_test, df_pred):
    """
    Train a model on training data, return a trained model and an evaluation df
    :param model:
    :param X_train: training features dataframe, the output from encode_data func or preproc_data func
    :param y_train: training label
    :param X_test: test features dataframe
    :param df_pred: the last output from function split_data, a df with columns 'ID', 'YEAR_MONTH', 'ACTUAL'
    :return:
    """
    tic = time.time()
    model.fit(X_train, y_train)
    toc = time.time()
    train_time = round(toc - tic, 2)
    print("Training time: {}".format(time.strftime("%H:%M:%S", time.gmtime(train_time))))

    tic_p = time.time()
    pred = model.predict(X_test)
    toc_p = time.time()
    pred_time = round(toc_p - tic_p, 2)
    print("Predicting time: {}".format(time.strftime("%H:%M:%S", time.gmtime(pred_time))))

    df_pred['PRED'] = pred
    # df_ev = evaluate(df_pred)

    return train_time, pred_time, model #df_ev,