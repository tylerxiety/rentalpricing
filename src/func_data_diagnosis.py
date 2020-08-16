# Here saves the functions for data diagnosis.

import pandas as pd
import sys
import numpy as np

def cleanse_data(df):
    """
    Take the input data and cleanse it by handling all the data issues
    :param df: a dataframe of raw data
    :return: a cleansed dataframe
    """
    # make a copy
    df_cp = df.copy()

    # col name uppercase
    df_cp.columns = df_cp.columns.str.upper()

    # tide col names
    colname_map = {'CALCULATED_HOST_LISTINGS_COUNT': 'HOST_LISTINGS_COUNT',
                   'CALCULATED_HOST_LISTINGS_COUNT_ENTIRE_HOMES': 'HOST_ENTIRE_HOMES',
                   'CALCULATED_HOST_LISTINGS_COUNT_PRIVATE_ROOMS': 'HOST_PRIVATE_ROOMS',
                   'CALCULATED_HOST_LISTINGS_COUNT_SHARED_ROOMS': 'HOST_SHARED_ROOMS',
                   'LAST_SCRAPED': 'SCRAPED_DATE'}
    df_cp = df_cp.rename(columns=colname_map)

    # str columns lowercase
    df_cp.PROPERTY_TYPE = df_cp.PROPERTY_TYPE.str.lower()
    df_cp.ROOM_TYPE = df_cp.ROOM_TYPE.str.lower()
    df_cp.BED_TYPE = df_cp.BED_TYPE.str.lower()
    df_cp.CANCELLATION_POLICY = df_cp.CANCELLATION_POLICY.str.lower()

    # convert 't'and 'f' to 1 and 0
    df_cp.INSTANT_BOOKABLE = df_cp.INSTANT_BOOKABLE.replace({'t': 1, 'f': 0})
    df_cp.REQUIRE_GUEST_PROFILE_PICTURE = df_cp.REQUIRE_GUEST_PROFILE_PICTURE.replace({'t': 1, 'f': 0})
    df_cp.REQUIRE_GUEST_PHONE_VERIFICATION = df_cp.REQUIRE_GUEST_PHONE_VERIFICATION.replace({'t': 1, 'f': 0})
    df_cp.HOST_IS_SUPERHOST = df_cp.HOST_IS_SUPERHOST.replace({'t': 1, 'f': 0})
    df_cp.HOST_HAS_PROFILE_PIC = df_cp.HOST_HAS_PROFILE_PIC.replace({'t': 1, 'f': 0})
    df_cp.HOST_IDENTITY_VERIFIED = df_cp.HOST_IDENTITY_VERIFIED.replace({'t': 1, 'f': 0})
    df_cp.IS_LOCATION_EXACT = df_cp.IS_LOCATION_EXACT.replace({'t': 1, 'f': 0})

    # convert str to num
    df_cp.HOST_RESPONSE_RATE = df_cp.HOST_RESPONSE_RATE.str.replace('%', '').astype(float)
    df_cp.HOST_ACCEPTANCE_RATE = df_cp.HOST_ACCEPTANCE_RATE.str.replace('%', '').astype(float)

    return df_cp


def check_dataframe(df, cols):
    """
    Check if the input dataframe has the required columns
    :param df: input dataframe
    :param cols: a list of required columns
    :return:
    """
    # check if input is a dataframe
    if not isinstance(df, pd.DataFrame):
        sys.exit('Input is not a data frame!')
    # check if column names are all there
    elif not all(col in list(df.columns) for col in cols):
        list_missing = np.setdiff1d(cols, list(df.columns)).tolist()
        sys.exit('Missing columns: {}. Please check!'.format(", ".join(list_missing)))
    # check if the input is a non-empty data frame
    elif df.shape[0] < 1:
        sys.exit('Input is an empty data frame!')
    else:
        return None


def check_dictionary(dict_params, keys):
    """
    Check if input is a dictionary
    :param dict_params:
    :param keys:
    :return:
    """
    if not isinstance(dict_params, dict):
        sys.exit('Input is not a dictionary!')
    # check if dict_params has the required fields, i.e. experiment_id, comment
    elif not all(k in dict_params.keys() for k in keys):
        list_missing = np.setdiff1d(keys, list(dict_params.keys())).tolist()
        sys.exit('Missing keys in dict_params: {}. Please check!'.format(", ".join(list_missing)))
    else:
        return None