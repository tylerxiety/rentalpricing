from src.func_data_diagnosis import *
from src.func_data_engineering import *
import glob
import os

if __name__ == "__main__":

    # read listing data
    log_time("Read listing data")
    path = r'data\20'
    listing_name = "listings*.csv"
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
        , 'number_of_reviews', 'number_of_reviews_ltm', 'first_review','last_review'
        , 'review_scores_rating', 'review_scores_accuracy', 'review_scores_cleanliness'
        , 'review_scores_checkin', 'review_scores_communication', 'review_scores_location'
        , 'review_scores_value', 'reviews_per_month'
        # prices
        , 'price', 'weekly_price', 'monthly_price', 'security_deposit', 'cleaning_fee'
        , 'guests_included', 'extra_people'
        # location
        , 'street','neighbourhood_cleansed', 'city', 'state'
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

    listings = read_data(path,listing_name, 'last_scraped', cols)
    df_listing = pd.concat(listings, axis=0, ignore_index=True)

    # read calendar data
    log_time("Read calendar data")
    cal_name = "calendar*.csv"
    calendars = read_data(path, cal_name, 'date')

    # for each monthly scrape, only keep the calendar data b/w the scraped date and the date of next scrape run
    cal_months = []
    num_cal = len(calendars)
    for i in range(num_cal):
        if i < num_cal - 1:
            date_end = calendars[i + 1].SCRAPED_DATE[0]
            df = calendars[i]
            df = df[df.date < date_end]
        else:
            df = calendars[i]
            date_start = pd.to_datetime(df.SCRAPED_DATE[0]).date()
            date_end = date_start + relativedelta.relativedelta(months=1)
            df = df[df.date < str(date_end)]
        cal_months.append(df)

    df_cal = pd.concat(cal_months, axis=0, ignore_index=True)


    # cleanse
    log_time("Cleanse listing data")
    df_listing = cleanse_data(df_listing)
    log_time("Cleanse calendar data")
    df_cal = cleanse_data(df_cal, list_data = False)

    # aggregate calendar data to monthly
    log_time("Aggregate calendar data to monthly")
    df_cal = agg_to_monthly(df_cal)

    # merge listing and aggregated calendar together
    df_listing['YEAR_MONTH'] = df_listing.SCRAPED_DATE.str[:7]
    df_data = df_cal.merge(df_listing, on=['ID','YEAR_MONTH'])

    # todo: check and drop duplicate

    # imputation?

    # feature store
    log_time('Feature engineering - listing')
    df_list = fs_listing(df_data, output_all = True)

    log_time('Feature engineering - price')
    df_price = fs_price(df_data, monthly=True)

    log_time('Feature engineering - calendar')
    df_calendar = fs_calendar(df_data, output_all=True)

    log_time('Feature engineering - booked')
    df_booked = fs_booked(df_data, output_all=True)

    # log_time('Feature engineering - review')
    # df_list = fs_list(df, output_all=True)
    #
    # log_time('Feature engineering - location')
    # df_list = fs_list(df, output_all=True)
    #
    # log_time('Feature engineering - host')
    # df_list = fs_list(df, output_all=True)
    #
    # log_time('Feature engineering - time')
    # df_list = fs_list(df, output_all=True)




