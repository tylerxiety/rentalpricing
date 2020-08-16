# Here saves data engineering functions

from datetime import datetime
import yaml
from sqlalchemy import create_engine
import time
import numpy as np




def log_time(msg):
    print(msg + ": " + datetime.now().strftime("%d/%m/%Y %H:%M:%S"))



def connect_my_db(secrets):
    """
    connect to the database
    :param secrets: path of where the connection string is
    :return: db connection (engine from sqlalchemy
    """
    with open(secrets) as file:
        uri = yaml.load(file, Loader = yaml.FullLoader)
    uri = uri["db_string"]

    return create_engine(uri)


def upload_df(engine, df, table_name):
    """
    upload the table to PostgreSQL database
    :param engine: the connection engine from func connect_my_db
    :param df: a data frame to be uploaded
    :param table_name: name of the table to be uploaded to
    :return: time it takes
    """
    tik = time.time()

    df.to_sql(table_name, engine, if_exists = 'replace', index = False)
    tok = time.time()

    return tok - tik



def fs_list(df):
    """
    Take the input data and create features for the listing dimensions
    :param df: a cleansed listing data frame
    :return: a data frame with ONLY the features for the listing dimensions
    """
    # select listing features
    df_list = df[['ID', 'HOST_ID', 'SCRAPED_DATE',
                     'PROPERTY_TYPE', 'ROOM_TYPE', 'ACCOMMODATES',
                     'BATHROOMS', 'BEDROOMS', 'BEDS', 'BED_TYPE', 'AMENITIES', 'SQUARE_FEET',
                     'INSTANT_BOOKABLE', 'IS_BUSINESS_TRAVEL_READY', 'GUESTS_INCLUDED',
                     'CANCELLATION_POLICY', 'REQUIRE_GUEST_PROFILE_PICTURE',
                     'REQUIRE_GUEST_PHONE_VERIFICATION',
                     'LUXURY_FLAG', 'NEIGHBOURHOOD_CLEANSED', 'LATITUDE', 'LONGITUDE',
                     'IS_LOCATION_EXACT', ]]

    # 'aggregate' some features
    # todo: experiment on keep more values
    df_list.PROPERTY_TYPE = df_list.PROPERTY_TYPE.replace(['loft', 'guest suite', 'condominium'], 'apartment')
    df_list.PROPERTY_TYPE = df_list.PROPERTY_TYPE.replace(
        ['townhouse', 'guesthouse', 'cottage', 'bungalow', 'cabin', 'tiny house', 'nature lodge', 'villa',
         'earth house', 'castle',
         'treehouse', 'chalet', 'lighthouse'], 'house')
    df_list.PROPERTY_TYPE = df_list.PROPERTY_TYPE.replace(
        ['farm stay', 'bed and breakfast', 'barn', 'train', 'boat', 'camper/rv', 'tent', 'hut', 'kezhan (china)',
         'casa particular (cuba)', 'campsite', 'dome house', 'tipi'], 'other')
    df_list.PROPERTY_TYPE = df_list.PROPERTY_TYPE.replace(
        ['boutique hotel', 'serviced apartment', 'hotel', 'hostel', 'aparthotel'], 'serviced')

    # create luxury_flag
    df_list['LUXURY_FLAG'] = np.where(df_list.CANCELLATION_POLICY.str.contains('luxury'), 1, 0)

    return df_list


def fs_host(df):
    """
    Take the input data and create features for the host dimensions
    :param df: a cleansed listing data frame
    :return: a data frame with ONLY the features for the host dimensions
    """
    # make a copy
    df_host = df[['SCRAPED_DATE', 'HOST_ID', 'HOST_SINCE', 'HOST_NEIGHBOURHOOD',
                       'HOST_RESPONSE_TIME', 'HOST_RESPONSE_RATE', 'HOST_ACCEPTANCE_RATE',
                       'HOST_IS_SUPERHOST', 'HOST_LISTINGS_COUNT',
                       'HOST_ENTIRE_HOMES',
                       'HOST_PRIVATE_ROOMS',
                       'HOST_SHARED_ROOMS', 'HOST_VERIFICATIONS',
                       'HOST_HAS_PROFILE_PIC', 'HOST_IDENTITY_VERIFIED']]

    return df_host


def fs_review(df):
    """
    Take the input data and create features for the review dimensions
    :param df: a cleansed listing data frame
    :return: a data frame with ONLY the features for the review dimensions
    """
    # make a copy
    df_review = df[['ID', 'SCRAPED_DATE','NUMBER_OF_REVIEWS',
                     'FIRST_REVIEW',
       'LAST_REVIEW', 'REVIEW_SCORES_RATING', 'REVIEW_SCORES_ACCURACY',
       'REVIEW_SCORES_CLEANLINESS', 'REVIEW_SCORES_CHECKIN',
       'REVIEW_SCORES_COMMUNICATION', 'REVIEW_SCORES_LOCATION',
       'REVIEW_SCORES_VALUE','REVIEWS_PER_MONTH']]

    return df_review


def fs_location(df):
    """
    Take the input data and create features for the location dimensions
    :param df: a cleansed listing data frame
    :return: a data frame with ONLY the features for the review dimensions
    """
    # make a copy
    df_location = df[['ID', 'SCRAPED_DATE','STREET','NEIGHBOURHOOD_CLEANSED','CITY', 'STATE', 'ZIPCODE', 'MARKET',
       'SMART_LOCATION', 'COUNTRY', 'LATITUDE', 'LONGITUDE',
       'IS_LOCATION_EXACT']]

    return df_location