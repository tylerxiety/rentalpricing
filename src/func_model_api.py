# Here saves the functions for model api

import time
from src.func_data_diagnosis import *

def global_model_api(df):
    """
    Take the input dataframe and predict the demand (number of nights booked)
    :param df: a dataframe with columns: ACCOMMODATES...NEIGHBOURHOOD_CLEANSED
    :return:
    """

    tic = time.time()

    # input validation
    cols = ['ACCOMMODATES', 'BATHROOMS', 'BEDROOMS', 'BEDS', 'INSTANT_BOOKABLE'
        , 'IS_BUSINESS_TRAVEL_READY', 'GUESTS_INCLUDED', 'REQUIRE_GUEST_PROFILE_PICTURE'
        , 'REQUIRE_GUEST_PHONE_VERIFICATION', 'LUXURY_FLAG', 'LATITUDE', 'LONGITUDE'
        , 'IS_LOCATION_EXACT', 'ADJUSTED_PRICE', 'HOST_RESPONSE_RATE'
        , 'HOST_ACCEPTANCE_RATE', 'HOST_IS_SUPERHOST', 'HOST_LISTINGS_COUNT', 'HOST_HAS_PROFILE_PIC'
        , 'HOST_IDENTITY_VERIFIED', 'YEAR', 'MONTH', 'HOST_MONTHS', 'NUMBER_OF_REVIEWS'
        , 'REVIEW_SCORES_RATING''REVIEWS_PER_MONTH'
        ,'PROPERTY_TYPE', 'ROOM_TYPE', 'BED_TYPE', 'CANCELLATION_POLICY'
        , 'NEIGHBOURHOOD_CLEANSED']
    check_dataframe(df, cols)

    # todo: read the raw data for lag features, if needed
    # append df to df_data
    # ensure data type

    # feature engineering (the final feature engineering function)


