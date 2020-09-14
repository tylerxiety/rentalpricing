# Here saves the functions for model api

import time
from src.func_data_diagnosis import *
from src.func_data_engineering import *
import joblib


def global_model_api(df, pred_time = False):
    """
    Take the input dataframe and predict the demand (number of nights booked)
    :param df: a dataframe with columns: ACCOMMODATES...NEIGHBOURHOOD_CLEANSED
    :return: a dataframe with the columns: ID, YEAR_MONTH, PRICE, BOOKED
    """
    # record
    tic = time.time()

    # input validation
    col_list = [
        # list
        'ID', 'YEAR_MONTH', 'PROPERTY_TYPE', 'ROOM_TYPE', 'ACCOMMODATES'
        , 'BATHROOMS', 'BEDROOMS', 'BEDS', 'BED_TYPE', 'SQUARE_FEET'
        , 'INSTANT_BOOKABLE', 'GUESTS_INCLUDED', 'CANCELLATION_POLICY'
        , 'REQUIRE_GUEST_PROFILE_PICTURE', 'REQUIRE_GUEST_PHONE_VERIFICATION'
        # cal
        , 'AVG_MINIMUM_NIGHTS', 'AVG_MAXIMUM_NIGHTS'
        # host
        , 'HOST_RESPONSE_RATE', 'HOST_SINCE'
        , 'HOST_ACCEPTANCE_RATE', 'HOST_IS_SUPERHOST', 'HOST_LISTINGS_COUNT'
        , 'HOST_HAS_PROFILE_PIC', 'HOST_IDENTITY_VERIFIED'
        # review
        , 'NUMBER_OF_REVIEWS', 'FIRST_REVIEW', 'LAST_REVIEW'
        , 'REVIEW_SCORES_RATING', 'REVIEW_SCORES_ACCURACY'
        , 'REVIEW_SCORES_CLEANLINESS', 'REVIEW_SCORES_CHECKIN'
        , 'REVIEW_SCORES_COMMUNICATION', 'REVIEW_SCORES_LOCATION'
        , 'REVIEW_SCORES_VALUE', 'REVIEWS_PER_MONTH'
        # location
        , 'NEIGHBOURHOOD_CLEANSED', 'IS_LOCATION_EXACT'
        # , 'LATITUDE', 'LONGITUDE'
        # price
        , 'TXN_PRICE', 'SECURITY_DEPOSIT', 'CLEANING_FEE', 'EXTRA_PEOPLE'
    ]
    check_dataframe(df, col_list)

    # make a copy
    df_cp = df.copy()

    # read the raw data for lag features
    df_data = pd.read_csv('data/test_mar_to_may20.csv', low_memory=False)
    df_data = df_data[col_list+['BOOKED']]

    # append df to df_data
    df_data = df_data.append(df_cp, ignore_index=True)
    # ensure data type

    # feature engineering
    df_data = fs_final(df_data, output_all=True)

    # subset to only df rows
    min_date = df_cp.YEAR_MONTH.min()
    df_data = df_data[df_data.YEAR_MONTH >= min_date]

    # read ohe transfermor and encode data
    ohe = joblib.load("model/ohe_base.ohe")
    col_cate = ['PROPERTY_TYPE', 'ROOM_TYPE', 'BED_TYPE', 'CANCELLATION_POLICY'
    , 'NEIGHBOURHOOD']
    # encode
    df_ohe = pd.DataFrame(ohe.transform(df_data[col_cate]))
    # get index back
    df_ohe.index = df_data.index
    # put the encoded columns and numerical columns together
    df_num = df_data.drop(col_cate, axis=1)
    df_data = pd.concat([df_num, df_ohe], axis=1)

    # read model and predict
    model = joblib.load('model/model_base.model')

    df_pred = df_data[['ID','YEAR_MONTH','TXN_PRICE']]
    df_data = df_data.drop(['ID', 'YEAR_MONTH', 'BOOKED'], axis=1)
    df_pred['BOOKED'] = model.predict(df_data)

    # output validation
    # deal with None and negatives
    df_pred.BOOKED.replace(np.inf, 0)
    df_pred.BOOKED.fillna(0)
    df_pred.BOOKED[df_pred.BOOKED < 0].BOOKED = 0
    df_pred.BOOKED = df_pred.BOOKED.astype(int)
    # ensure data type, if needed

    toc = time.time()
    api_time = round(toc - tic, 2)

    if pred_time:
        return df_pred, api_time
    else:
        return df_pred




