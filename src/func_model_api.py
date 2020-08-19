# Here saves the functions for model api

import time
from src.func_data_diagnosis import *
import joblib


def global_model_api(df, pred_time = False):
    """
    Take the input dataframe and predict the demand (number of nights booked)
    :param df: a dataframe with columns: ACCOMMODATES...NEIGHBOURHOOD_CLEANSED
    :return: a dataframe with the same columns: ACCOMMODATES...NEIGHBOURHOOD_CLEANSED, plus BOOKED
    """

    tic = time.time()

    # input validation
    col_list = ['ACCOMMODATES', 'BATHROOMS', 'BEDROOMS', 'BEDS', 'INSTANT_BOOKABLE'
        , 'GUESTS_INCLUDED', 'REQUIRE_GUEST_PROFILE_PICTURE'
        , 'REQUIRE_GUEST_PHONE_VERIFICATION', 'LUXURY_FLAG', 'LATITUDE', 'LONGITUDE'
        , 'IS_LOCATION_EXACT', 'ADJUSTED_PRICE', 'HOST_RESPONSE_RATE'
        , 'HOST_ACCEPTANCE_RATE', 'HOST_IS_SUPERHOST', 'HOST_LISTINGS_COUNT', 'HOST_HAS_PROFILE_PIC'
        , 'HOST_IDENTITY_VERIFIED', 'YEAR', 'MONTH', 'HOST_MONTHS', 'NUMBER_OF_REVIEWS'
        , 'REVIEW_SCORES_RATING', 'REVIEWS_PER_MONTH'

        , 'PROPERTY_TYPE', 'ROOM_TYPE', 'BED_TYPE', 'CANCELLATION_POLICY'
        , 'NEIGHBOURHOOD_CLEANSED']
    check_dataframe(df, col_list)

    # make a copy
    df_cp = df.copy()

    # todo: read the raw data for lag features, if needed
    # append df to df_data
    # ensure data type

    # feature engineering (the final feature engineering function)
    # read ohe transfermor and encode data
    ohe = joblib.load("model/ohe_base.ohe")
    col_cate = ['PROPERTY_TYPE', 'ROOM_TYPE', 'BED_TYPE', 'CANCELLATION_POLICY'
    , 'NEIGHBOURHOOD_CLEANSED']
    # encode
    df_ohe = pd.DataFrame(ohe.transform(df_cp[cols]))
    # get index back
    df_ohe.index = df_cp.index
    # put the encoded columns and numerical columns together
    df_num = df_cp.drop(col_cate, axis=1)
    df_data = pd.concat([df_num, df_ohe], axis=1)

    # read model and predict

    model = joblib.load('model/model_base.model')

    df_pred = df_data[col_list]

    df_pred['BOOKED'] = model.predict(df_data)

    # output validation
    # deal with None and negatives
    df_pred.BOOKED.replace(np.inf, 0)
    df_pred.BOOKED.fillna(0)
    df_pred.BOOKED[df_pred.BOOKED < 0].BOOKED = 0

    # ensure data type, if needed

    toc = time.time()
    pred_time = round(toc - tic, 2)

    if pred_time == True:
        return df_pred, pred_time
    else:
        return df_pred




