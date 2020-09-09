# Here saves data engineering functions

from datetime import datetime
import yaml
from sqlalchemy import create_engine
import time
import numpy as np
import pandas as pd
from dateutil import relativedelta
import glob
import os
import sys


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

def read_table(engine, table_name, date_col = 'YEAR_MONTH', date_start = None, date_end = None):
    """
    Read the table from PostgreSQL
    :param engine: the connection engine from sqlalchemy
    :param table_name: name of the table
    :param date_start: start date the data to retrieve
    :param date_end: end date the data to retrieve
    :return: the data frame read from the PostgreSQL
    """
    if None not in (date_start, date_end):
        query = 'SELECT * FROM ' + f'"{table_name}" WHERE "{date_col}" between '+ f"'{date_start}' and '{date_end}'"
    else:
        query = 'SELECT * FROM ' + f'"{table_name}"'

    df = pd.read_sql(query, engine)
    return df

def read_tables(engine, table_names=["FS_LIST_MONTHLY","FS_CAL_MONTHLY"
    ,"FS_HOST_MONTHLY",'FS_REVIEW_MONTHLY'
    ,"FS_BOOKED_MONTHLY","FS_LOCATION_MONTHLY"
    ,"FS_TIME_MONTHLY","FS_PRICE_MONTHLY"], date_start = None, date_end = None):
    """
    read multiple tables and combine into one df
    :param engine:
    :param table_names:
    :param date_start:
    :param date_end:
    :return:
    """
    # initialize
    df_all = pd.DataFrame(columns=['ID','YEAR_MONTH'])

    for table in table_names:
        df = read_table(engine, table, date_start, date_end)
        df_all = df_all.merge(df, on = ['ID','YEAR_MONTH'],how='outer')

    return df_all


def read_data(path, file_names, col_scrape_date, listing_data = False):
    """
    Read multiple csv files and output a list of  dataframe
    :param path: file path
    :param file_names: file names in regex
    :param col_names: columns to output
    :param col_scrape_date: the column to be used to create scrape_date
    :return: a list of df
    """
    file_paths = glob.glob(os.path.join(path, file_names))
    list_df = []

    for file in file_paths:

        df = pd.read_csv(file, header=0, low_memory=False)
        if listing_data:
            cols = [
                # listing
                'id', 'last_scraped', 'property_type', 'room_type', 'accommodates'
                , 'bathrooms', 'bedrooms', 'beds', 'bed_type', 'amenities', 'square_feet'
                , 'instant_bookable', 'is_business_travel_ready', 'cancellation_policy'
                , 'require_guest_profile_picture', 'require_guest_phone_verification'
                , 'guests_included'
                # calendar
                , 'minimum_nights', 'maximum_nights', 'minimum_minimum_nights', 'maximum_minimum_nights'
                , 'minimum_maximum_nights', 'maximum_maximum_nights', 'minimum_nights_avg_ntm'
                , 'maximum_nights_avg_ntm', 'calendar_updated', 'has_availability', 'availability_30'
                , 'availability_60', 'availability_90', 'availability_365', 'calendar_last_scraped'
                # reviews
                , 'number_of_reviews', 'number_of_reviews_ltm', 'first_review', 'last_review'
                , 'review_scores_rating', 'review_scores_accuracy', 'review_scores_cleanliness'
                , 'review_scores_checkin', 'review_scores_communication', 'review_scores_location'
                , 'review_scores_value', 'reviews_per_month'
                # prices
                , 'price', 'weekly_price', 'monthly_price', 'security_deposit', 'cleaning_fee'
                , 'extra_people'
                # location
                , 'street', 'neighbourhood_cleansed', 'city', 'state'
                , 'zipcode', 'market', 'smart_location', 'country', 'latitude', 'longitude'
                , 'is_location_exact'
                # host
                , 'host_id', 'host_since', 'host_neighbourhood', 'host_response_time'
                , 'host_response_rate', 'host_acceptance_rate', 'host_is_superhost'
                , 'calculated_host_listings_count', 'calculated_host_listings_count_entire_homes'
                , 'calculated_host_listings_count_private_rooms'
                , 'calculated_host_listings_count_shared_rooms'
                , 'host_verifications', 'host_has_profile_pic', 'host_identity_verified'
            ]

            df = df[cols]
        df['SCRAPED_DATE'] = df[col_scrape_date].min()
        list_df.append(df)

    return list_df

def agg_to_monthly(df):
    """
    Aggregate calednar data to monthly
    :param df: calender daily data
    :return: a df of monthly calendar data
    """
    # make a copy
    df_cp = df.copy()

    df_cp['YEAR_MONTH'] = df_cp.DATE.str[:7]

    df_cp = df_cp.groupby(['ID', 'YEAR_MONTH']).agg({
        'BASE_PRICE': 'median'
        , 'TXN_PRICE': 'median'
        , 'BOOKED': 'sum'}).reset_index()

    return df_cp


def fs_listing(df, output_all = False):
    """
    Take the input data and create features for the listing dimensions
    :param df: a cleansed listing data frame
    :return: a data frame with ONLY the features for the listing dimensions
    """

    # make a copy
    df_cp = df.copy()

    # 'aggregate' some features
    # todo: experiment on keep more values
    df_cp.PROPERTY_TYPE = df_cp.PROPERTY_TYPE.replace(['loft', 'guest suite', 'condominium'], 'apartment')
    df_cp.PROPERTY_TYPE = df_cp.PROPERTY_TYPE.replace(
        ['townhouse', 'guesthouse', 'cottage', 'bungalow', 'cabin', 'tiny house', 'nature lodge', 'villa',
         'earth house', 'castle',
         'treehouse', 'chalet', 'lighthouse'], 'house')
    df_cp.PROPERTY_TYPE = df_cp.PROPERTY_TYPE.replace(
        ['farm stay', 'bed and breakfast', 'barn', 'train', 'boat', 'camper/rv', 'tent', 'hut', 'kezhan (china)',
         'casa particular (cuba)', 'campsite', 'dome house', 'tipi'], 'other')
    df_cp.PROPERTY_TYPE = df_cp.PROPERTY_TYPE.replace(
        ['boutique hotel', 'serviced apartment', 'hotel', 'hostel', 'aparthotel'], 'serviced')

    # create luxury_flag
    df_cp['LUXURY_FLAG'] = np.where(df_cp.CANCELLATION_POLICY.str.contains('luxury'), 1, 0)

    if output_all:

        cols = ['ID', 'YEAR_MONTH', 'PROPERTY_TYPE', 'ROOM_TYPE', 'ACCOMMODATES'
            , 'BATHROOMS', 'BEDROOMS', 'BEDS', 'BED_TYPE', 'AMENITIES', 'SQUARE_FEET'
            , 'INSTANT_BOOKABLE', 'IS_BUSINESS_TRAVEL_READY', 'GUESTS_INCLUDED'
            , 'CANCELLATION_POLICY', 'REQUIRE_GUEST_PROFILE_PICTURE'
            , 'REQUIRE_GUEST_PHONE_VERIFICATION', 'LUXURY_FLAG']
        df_listing = df_cp[cols]
        return df_listing

    else:
        return df_cp

def fs_time(df, output_all = False):
    """
    Take the input data and create features for the time dimension
    :param df: a cleansed listing data frame
    :return: a data frame with ONLY the features for the time dimension
    """
    # make a copy
    df_cp = df.copy()

    # get year and month
    df_cp['YEAR'] = df_cp.YEAR_MONTH.str[:4].astype(int)
    df_cp['MONTH'] = df_cp.YEAR_MONTH.str[5:7].astype(int)

    df_cp["SCRAPED_DATE"] = pd.to_datetime(df_cp.SCRAPED_DATE)
    df_cp["QUARTER"] = df_cp["SCRAPED_DATE"].dt.quarter
    df_cp["YEAR_START"] = df_cp["SCRAPED_DATE"].dt.is_year_start
    df_cp["YEAR_END"] = df_cp["SCRAPED_DATE"].dt.is_year_end


    # todo: add number of holidays each month

    if output_all:
        cols = ['ID', 'YEAR_MONTH', 'MONTH','QUARTER','YEAR','YEAR_START','YEAR_END']
        df_time = df_cp[cols]
        return df_time
    else:
        return df_cp


def fs_price(df, monthly = False):
    """
    Take the input data and create features for the price dimension
    :param df: a cleansed listing data frame
    :return: a data frame with ONLY the features for the price dimension
    """
    # make a copy
    df_cp = df.copy()
    df_cp['PRICE_PER_GUEST'] = df_cp.TXN_PRICE/df_cp.GUESTS_INCLUDED
    #lag
    df_cp["PRICE_LAG_1"] = df_cp.sort_values(by='YEAR_MONTH').groupby(["ID"])["TXN_PRICE"].shift(1)
    df_cp["PRICE_LAG_2"] = df_cp.sort_values(by='YEAR_MONTH').groupby(["ID"])["TXN_PRICE"].shift(2)
    df_cp["PRICE_LAG_3"] = df_cp.sort_values(by='YEAR_MONTH').groupby(["ID"])["TXN_PRICE"].shift(3)
    # minus lag
    df_cp["PRICE_MINUS_LAG_1"] = df_cp.TXN_PRICE - df_cp.PRICE_LAG_1
    df_cp["PRICE_MINUS_LAG_2"] = df_cp.TXN_PRICE - df_cp.PRICE_LAG_2
    df_cp["PRICE_MINUS_LAG_3"] = df_cp.TXN_PRICE - df_cp.PRICE_LAG_3
    # moving average/median
    pma3 = df_cp.sort_values(by='YEAR_MONTH').groupby(["ID"])["TXN_PRICE"].rolling(window=3).median()
    df_cp['PRICE_MA_3'] = pma3.reset_index(level=0, drop=True).reset_index(level=0, drop=True)

    # minus moving average
    df_cp["PRICE_MINUS_MA_3"] = df_cp.TXN_PRICE - df_cp.PRICE_MA_3


    if monthly:
        cols = ['ID', 'YEAR_MONTH','TXN_PRICE', 'PRICE_PER_GUEST'
            , 'SECURITY_DEPOSIT','CLEANING_FEE','EXTRA_PEOPLE'
            , 'PRICE_LAG_1', 'PRICE_LAG_2', 'PRICE_LAG_3'
            , 'PRICE_MINUS_LAG_1','PRICE_MINUS_LAG_2','PRICE_MINUS_LAG_3'
            , 'PRICE_MA_3','PRICE_MINUS_MA_3']

    else:
        cols = ['ID', 'DATE','BASE_PRICE', 'TXN_PRICE', 'PRICE_PER_GUEST'
            ,'MONTHLY_PRICE', 'WEEKLY_PRICE'
            ,'SECURITY_DEPOSIT','CLEANING_FEE','EXTRA_PEOPLE',]

    df_price = df_cp[cols]
    return df_price



def calculate_months(date1, date2):
    """
    calcualte months between two dates
    :param date1: date str
    :param date2: date str
    :return: months difference b/w the dates
    """
    for date in [date1, date2]:

        if pd.isnull(date):
            return None

    r = relativedelta.relativedelta(pd.to_datetime(date1), pd.to_datetime(date2))
    months = 12*abs(r.years)+abs(r.months)

    return months


    # for date in [date1, date2]:
    #
    #     if pd.isnull(date):
    #         return None
    #
    #     elif isinstance(date, str):
    #         date = pd.to_datetime(date)
    #
    # if isinstance(date2, datetime) & isinstance(date1, datetime):
    #
    #     r = relativedelta.relativedelta(date1, date2)
    #     months = 12*abs(r.years)+abs(r.months)
    #
    #     return months
    # else:
    #     sys.exit('Wrong input!')

def fs_calendar(df, output_all = False):
    """
    Take the input data and create features for the calendar dimension
    :param df: a cleansed listing data frame
    :return: a data frame with ONLY the features for the calendar dimension
    """
    # make a copy
    df_cp = df.copy()


    if output_all:
        cols = ['ID', 'YEAR_MONTH', 'MAX_MINIMUM_NIGHTS', 'MIN_MINIMUM_NIGHTS'
                ,'MIN_MAXIMUM_NIGHTS','MAX_MAXIMUM_NIGHTS','AVG_MINIMUM_NIGHTS'
                ,'AVG_MAXIMUM_NIGHTS', 'CALENDAR_UPDATED','AVAILABILITY_60'
                ,'AVAILABILITY_90','AVAILABILITY_365']
        df_cal = df_cp[cols]
        return df_cal
    else:
        return df_cp

def fs_booked(df, output_all = False):
    """
    Take the input data and create features for the booked dimension
    :param df: a cleansed listing data frame
    :return: a data frame with ONLY the features for the booked dimension
    """
    # make a copy
    df_cp = df.copy()
    df_cp["BOOKED_LAG_1"] = df_cp.sort_values(by='YEAR_MONTH').groupby(["ID"])["BOOKED"].shift(1)
    df_cp["BOOKED_LAG_3"] = df_cp.sort_values(by='YEAR_MONTH').groupby(["ID"])["BOOKED"].shift(3)
    df_cp["BOOKED_LAG1_MINUS_LAG3"] = df_cp.BOOKED_LAG_1 - df_cp.BOOKED_LAG_3

    # lag1 moving average/median
    ma3 = df_cp.sort_values(by='YEAR_MONTH').groupby(["ID"])["BOOKED_LAG_1"].rolling(window=3).median()
    df_cp['BOOKED_LAG1_MA3'] = ma3.reset_index(level=0, drop=True).reset_index(level=0, drop=True)

    # minus moving average
    df_cp["BOOKED_LAG1_MINUS_LAG1MA3"] = df_cp.BOOKED_LAG_1 - df_cp.BOOKED_LAG1_MA3

    if output_all:
        cols = ['ID', 'YEAR_MONTH','BOOKED','BOOKED_LAG_1','BOOKED_LAG_3','BOOKED_LAG1_MINUS_LAG3'
                ,'BOOKED_LAG1_MA3','BOOKED_LAG1_MINUS_LAG1MA3']
        df_booked = df_cp[cols]
        return df_booked
    else:
        return df_cp


def fs_review(df, output_all = False):
    """
    Take the input data and create features for the review dimension
    :param df: a cleansed listing data frame
    :return: a data frame with ONLY the features for the review dimension
    """
    # make a copy
    df_cp = df.copy()

    for col in ['FIRST_REVIEW', 'LAST_REVIEW']:
        df_cp[f'MONTH_SINCE_{col}'] = df_cp.apply(lambda row: calculate_months(
            row['SCRAPED_DATE'], row[col]), axis=1)

    if output_all:
        cols = ['ID', 'YEAR_MONTH', 'NUMBER_OF_REVIEWS', 'MONTH_SINCE_FIRST_REVIEW'
            , 'MONTH_SINCE_LAST_REVIEW', 'REVIEW_SCORES_RATING', 'REVIEW_SCORES_ACCURACY'
            , 'REVIEW_SCORES_CLEANLINESS', 'REVIEW_SCORES_CHECKIN', 'REVIEW_SCORES_COMMUNICATION'
            , 'REVIEW_SCORES_LOCATION', 'REVIEW_SCORES_VALUE', 'REVIEWS_PER_MONTH']
        df_review = df_cp[cols]
        return df_review
    else:
        return df_cp



def fs_location(df, output_all = False):
    """
    Take the input data and create features for the location dimension
    :param df: a cleansed listing data frame
    :return: a data frame with ONLY the features for the location dimension
    """
    # make a copy
    df_cp = df.copy()

    df_cp = df_cp.rename(columns={'NEIGHBOURHOOD_CLEANSED':'NEIGHBOURHOOD'})

    if output_all:
        cols = ['ID', 'YEAR_MONTH', 'STREET', 'NEIGHBOURHOOD'
            , 'CITY', 'STATE', 'ZIPCODE', 'SMART_LOCATION'
            , 'COUNTRY', 'LATITUDE', 'LONGITUDE', 'IS_LOCATION_EXACT']
        df_location = df_cp[cols]
        return df_location
    else:
        return df_cp

# def fs_host(df):
#     """
#     Take the input data and create features for the host dimensions
#     :param df: a cleansed listing data frame
#     :return: a data frame with ONLY the features for the host dimensions
#     """
#     # make a copy
#     df_host = df[['SCRAPED_DATE', 'HOST_ID', 'HOST_SINCE', 'HOST_NEIGHBOURHOOD',
#                        'HOST_RESPONSE_TIME', 'HOST_RESPONSE_RATE', 'HOST_ACCEPTANCE_RATE',
#                        'HOST_IS_SUPERHOST', 'HOST_LISTINGS_COUNT',
#                        'HOST_ENTIRE_HOMES',
#                        'HOST_PRIVATE_ROOMS',
#                        'HOST_SHARED_ROOMS', 'HOST_VERIFICATIONS',
#                        'HOST_HAS_PROFILE_PIC', 'HOST_IDENTITY_VERIFIED']]
#     # convert HOST_SINCE to the number of months hosted
#     df_host['HOST_MONTHS'] = df_host.apply(lambda row: calculate_months(
#         pd.to_datetime(row['SCRAPED_DATE']), pd.to_datetime(row['HOST_SINCE'])), axis=1)
#
#     return df_host

def fs_host(df, output_all = False):
    """
    Take the input data and create features for the host dimension
    :param df: a cleansed listing data frame
    :return: a data frame with ONLY the features for the host dimension
    """
    # make a copy
    df_cp = df.copy()

    df_cp['HOST_MONTHS'] = df_cp.apply(lambda row: calculate_months(row['SCRAPED_DATE']
                                                                    , row['HOST_SINCE']), axis=1)

    if output_all:
        cols = ['ID', 'YEAR_MONTH', 'HOST_MONTHS', 'HOST_NEIGHBOURHOOD'
            , 'HOST_RESPONSE_TIME', 'HOST_RESPONSE_RATE', 'HOST_ACCEPTANCE_RATE'
            , 'HOST_IS_SUPERHOST', 'HOST_LISTINGS_COUNT', 'HOST_ENTIRE_HOMES'
            , 'HOST_PRIVATE_ROOMS', 'HOST_SHARED_ROOMS', 'HOST_VERIFICATIONS'
            , 'HOST_HAS_PROFILE_PIC', 'HOST_IDENTITY_VERIFIED']
        df_host = df_cp[cols]
        return df_host
    else:
        return df_cp

def fs_final(df, output_all = False):
    """
    Take the input data and create final features for model API
    :param df: the listing data frame
    :return: a data frame with all features need for model API
    """

    # make a copy
    df_cp = df.copy()

    # ensure data type

    #todo: impute missing month
    # listing features
    df_cp.PROPERTY_TYPE = df_cp.PROPERTY_TYPE.replace(['loft', 'guest suite', 'condominium'], 'apartment')
    df_cp.PROPERTY_TYPE = df_cp.PROPERTY_TYPE.replace(
        ['townhouse', 'guesthouse', 'cottage', 'bungalow', 'cabin', 'tiny house', 'nature lodge', 'villa',
         'earth house', 'castle',
         'treehouse', 'chalet', 'lighthouse'], 'house')
    df_cp.PROPERTY_TYPE = df_cp.PROPERTY_TYPE.replace(
        ['farm stay', 'bed and breakfast', 'barn', 'train', 'boat', 'camper/rv', 'tent', 'hut', 'kezhan (china)',
         'casa particular (cuba)', 'campsite', 'dome house', 'tipi'], 'other')
    df_cp.PROPERTY_TYPE = df_cp.PROPERTY_TYPE.replace(
        ['boutique hotel', 'serviced apartment', 'hotel', 'hostel', 'aparthotel'], 'serviced')
    # create luxury_flag
    df_cp['LUXURY_FLAG'] = np.where(df_cp.CANCELLATION_POLICY.str.contains('luxury'), 1, 0)

    # time features
    df_cp['YEAR'] = df_cp.YEAR_MONTH.str[:4].astype(int)
    df_cp['MONTH'] = df_cp.YEAR_MONTH.str[5:7].astype(int)
    df_cp["YEAR_MONTH"] = pd.to_datetime(df_cp.YEAR_MONTH)
    df_cp["QUARTER"] = df_cp["YEAR_MONTH"].dt.quarter
    df_cp["YEAR_START"] = df_cp["YEAR_MONTH"].dt.is_year_start
    df_cp["YEAR_END"] = df_cp["YEAR_MONTH"].dt.is_year_end

    # price features
    df_cp['PRICE_PER_GUEST'] = df_cp.TXN_PRICE / df_cp.GUESTS_INCLUDED
    # lag
    df_cp["PRICE_LAG_1"] = df_cp.sort_values(by='YEAR_MONTH').groupby(["ID"])["TXN_PRICE"].shift(1)
    df_cp["PRICE_LAG_2"] = df_cp.sort_values(by='YEAR_MONTH').groupby(["ID"])["TXN_PRICE"].shift(2)
    df_cp["PRICE_LAG_3"] = df_cp.sort_values(by='YEAR_MONTH').groupby(["ID"])["TXN_PRICE"].shift(3)
    # minus lag
    df_cp["PRICE_MINUS_LAG_1"] = df_cp.TXN_PRICE - df_cp.PRICE_LAG_1
    df_cp["PRICE_MINUS_LAG_2"] = df_cp.TXN_PRICE - df_cp.PRICE_LAG_2
    df_cp["PRICE_MINUS_LAG_3"] = df_cp.TXN_PRICE - df_cp.PRICE_LAG_3
    # moving average/median
    pma3 = df_cp.sort_values(by='YEAR_MONTH').groupby(["ID"])["TXN_PRICE"].rolling(window=3).median()
    df_cp['PRICE_MA_3'] = pma3.reset_index(level=0, drop=True).reset_index(level=0, drop=True)
    # minus moving average
    df_cp["PRICE_MINUS_MA_3"] = df_cp.TXN_PRICE - df_cp.PRICE_MA_3

    # booked features
    df_cp["BOOKED_LAG_1"] = df_cp.sort_values(by='YEAR_MONTH').groupby(["ID"])["BOOKED"].shift(1)
    df_cp["BOOKED_LAG_3"] = df_cp.sort_values(by='YEAR_MONTH').groupby(["ID"])["BOOKED"].shift(3)
    df_cp["BOOKED_LAG1_MINUS_LAG3"] = df_cp.BOOKED_LAG_1 - df_cp.BOOKED_LAG_3
    # lag1 moving average/median
    ma3 = df_cp.sort_values(by='YEAR_MONTH').groupby(["ID"])["BOOKED_LAG_1"].rolling(window=3).median()
    df_cp['BOOKED_LAG1_MA3'] = ma3.reset_index(level=0, drop=True).reset_index(level=0, drop=True)
    # minus moving average
    df_cp["BOOKED_LAG1_MINUS_LAG1MA3"] = df_cp.BOOKED_LAG_1 - df_cp.BOOKED_LAG1_MA3

    # location
    df_cp = df_cp.rename(columns={'NEIGHBOURHOOD_CLEANSED': 'NEIGHBOURHOOD'})

    # review features
    for col in ['FIRST_REVIEW', 'LAST_REVIEW']:
        df_cp[f'MONTH_SINCE_{col}'] = df_cp.apply(lambda row: calculate_months(
            row['YEAR_MONTH'], row[col]), axis=1)

    # host
    df_cp['HOST_MONTHS'] = df_cp.apply(lambda row: calculate_months(row['HOST_SINCE'],
                                                                    row['YEAR_MONTH']), axis=1)

    # put YEAR_MONTH back to str
    df_cp["YEAR_MONTH"] = df_cp.YEAR_MONTH.astype(str).str[:7]

    if output_all:
        cols = [
            # list
            'ID', 'YEAR_MONTH', 'PROPERTY_TYPE', 'ROOM_TYPE', 'ACCOMMODATES'
            , 'BATHROOMS', 'BEDROOMS', 'BEDS', 'BED_TYPE', 'SQUARE_FEET'
            , 'INSTANT_BOOKABLE', 'GUESTS_INCLUDED', 'CANCELLATION_POLICY'
            , 'REQUIRE_GUEST_PROFILE_PICTURE', 'REQUIRE_GUEST_PHONE_VERIFICATION'
            , 'LUXURY_FLAG'
            # cal
            , 'AVG_MINIMUM_NIGHTS', 'AVG_MAXIMUM_NIGHTS'
            # host
            , 'HOST_MONTHS', 'HOST_RESPONSE_RATE'
            , 'HOST_ACCEPTANCE_RATE', 'HOST_IS_SUPERHOST', 'HOST_LISTINGS_COUNT'
            , 'HOST_HAS_PROFILE_PIC', 'HOST_IDENTITY_VERIFIED'
            # review
            , 'NUMBER_OF_REVIEWS', 'MONTH_SINCE_FIRST_REVIEW'
            , 'MONTH_SINCE_LAST_REVIEW', 'REVIEW_SCORES_RATING', 'REVIEW_SCORES_ACCURACY'
            , 'REVIEW_SCORES_CLEANLINESS', 'REVIEW_SCORES_CHECKIN'
            , 'REVIEW_SCORES_COMMUNICATION', 'REVIEW_SCORES_LOCATION'
            , 'REVIEW_SCORES_VALUE', 'REVIEWS_PER_MONTH'
            # booked
            , 'BOOKED', 'BOOKED_LAG_1', 'BOOKED_LAG_3', 'BOOKED_LAG1_MINUS_LAG3'
            , 'BOOKED_LAG1_MA3', 'BOOKED_LAG1_MINUS_LAG1MA3'
            # location
            , 'NEIGHBOURHOOD', 'IS_LOCATION_EXACT'
            # , 'LATITUDE', 'LONGITUDE'
            # time
            , 'MONTH', 'QUARTER', 'YEAR', 'YEAR_START', 'YEAR_END'
            # price
            , 'TXN_PRICE', 'PRICE_PER_GUEST', 'SECURITY_DEPOSIT', 'CLEANING_FEE', 'EXTRA_PEOPLE'
            , 'PRICE_LAG_1', 'PRICE_LAG_2', 'PRICE_LAG_3', 'PRICE_MINUS_LAG_1'
            , 'PRICE_MINUS_LAG_2', 'PRICE_MINUS_LAG_3', 'PRICE_MA_3', 'PRICE_MINUS_MA_3'

        ]
        df_final = df_cp[cols]
        return df_final
    else:
        return df_cp


# def fs_(df, output_all = False):
#     """
#     Take the input data and create features for the time dimension
#     :param df: a cleansed listing data frame
#     :return: a data frame with ONLY the features for the time dimension
#     """
#     # make a copy
#     df_cp = df.copy()
#
#
#
#     if output_all:
#         cols = ['ID', 'YEAR_MONTH']
#         df_time = df_cp[cols]
#         return df_time
#     else:
#         return df_cp