# Here saves the functions for optimiser api
from src.func_optimisation import *


def optimiser_api(df, op_time=False):
    """
    A wrapper func. Take a dataframe with feature columns and return a dataframe with optimised price
    :param df: a dataframe with feature columns including price
    :return: a dataframe with the same columns plus .....
    """
    # track the time used
    tic = time.time()

    # make a copy
    df_cp = df.copy()

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
        , 'BASE_PRICE', 'SECURITY_DEPOSIT', 'CLEANING_FEE', 'EXTRA_PEOPLE'
    ]
    check_dataframe(df_cp, col_list)

    # ensure data type if needed
    # impute if needed

    # add prices
    df_prices = create_multiple_prices(df_cp)

    # get demand curve
    df_demand = get_demand_curve(df_prices)

    # create obj column
    # no extra cost for nights booked
    df_demand = create_objective_col(df_demand)

    # get the best prices
    df_best = optimise_brute_force(df_demand)

    toc = time.time()
    total_time = round(toc - tic)

    if op_time:
        return df_best, total_time
    else:
        return df_best