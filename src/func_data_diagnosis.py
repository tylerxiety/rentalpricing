# Here saves the functions for data diagnosis.

import pandas as pd
import sys
import numpy as np

def cleanse_data(df, list_data = True):
    """
    Take the input data and cleanse it by handling all the data issues
    :param df: a dataframe of raw listing data
    :param list_data: specify if the input data is listing data or calendar data
    :return: a cleansed dataframe
    """
    # make a copy
    df_cp = df.copy()

    # col name uppercase
    df_cp.columns = df_cp.columns.str.upper()

    if list_data:
        # tide col names
        colname_map = {'CALCULATED_HOST_LISTINGS_COUNT': 'HOST_LISTINGS_COUNT'
            , 'CALCULATED_HOST_LISTINGS_COUNT_ENTIRE_HOMES': 'HOST_ENTIRE_HOMES'
            , 'CALCULATED_HOST_LISTINGS_COUNT_PRIVATE_ROOMS': 'HOST_PRIVATE_ROOMS'
            , 'CALCULATED_HOST_LISTINGS_COUNT_SHARED_ROOMS': 'HOST_SHARED_ROOMS'
            , 'MINIMUM_MINIMUM_NIGHTS': 'MIN_MINIMUM_NIGHTS'
            , 'MAXIMUM_MINIMUM_NIGHTS': 'MAX_MINIMUM_NIGHTS'
            , 'MINIMUM_MAXIMUM_NIGHTS': 'MIN_MAXIMUM_NIGHTS'
            , 'MAXIMUM_MAXIMUM_NIGHTS': 'MAX_MAXIMUM_NIGHTS'
            , 'MINIMUM_NIGHTS_AVG_NTM': 'AVG_MINIMUM_NIGHTS'
            , 'MAXIMUM_NIGHTS_AVG_NTM': 'AVG_MAXIMUM_NIGHTS'
            }
        df_cp = df_cp.rename(columns=colname_map)

        # str columns lowercase
        for col in ['PROPERTY_TYPE','ROOM_TYPE','BED_TYPE','CANCELLATION_POLICY']:
            df_cp[col] = df_cp[col].str.lower()

        # convert 't'and 'f' to 1 and 0
        for col in ['INSTANT_BOOKABLE', 'REQUIRE_GUEST_PROFILE_PICTURE', 'REQUIRE_GUEST_PHONE_VERIFICATION'
                    , 'HOST_IS_SUPERHOST', 'HOST_HAS_PROFILE_PIC', 'HOST_IDENTITY_VERIFIED'
                    , 'IS_LOCATION_EXACT', 'IS_BUSINESS_TRAVEL_READY']:
            df_cp[col] = df_cp[col].replace({'t': 1, 'f': 0})

        # convert str to num
        for col in ['HOST_RESPONSE_RATE', 'HOST_ACCEPTANCE_RATE']:
            df_cp[col] = df_cp[col].str.replace('%', '').astype(float)

    else: # calendar data
        # tide col names
        colname_map = {'LISTING_ID': 'ID'
            , 'ADJUSTED_PRICE': 'TXN_PRICE'
            , 'PRICE': 'BASE_PRICE'}
        df_cp = df_cp.rename(columns=colname_map)

        # convert prices to float
        # convert to float to keep nan values

        for col in ['TXN_PRICE','BASE_PRICE']:
            df_cp[col] = df_cp[col].str.replace('$', '')
            df_cp[col] = df_cp[col].str.replace(',', '')
            df_cp[col] = df_cp[col].astype(float)

        # convert availability to booked
        df_cp['BOOKED'] = df_cp.AVAILABLE.replace({'t': 0, 'f': 1})

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