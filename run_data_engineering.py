from src.func_data_diagnosis import *
from src.func_data_engineering import *
import pandas as pd


if __name__ == "__main__":

    # read listing data
    log_time("Read listing data")
    path = r"data/20"
    listing_name = "listings*.csv"
    listings = read_data(path,listing_name, 'last_scraped', listing_data = True)
    # put into one df
    df_listing = pd.concat(listings, axis=0, ignore_index=True)

    # read calendar data
    log_time("Read calendar data")
    cal_name = "calendar*.csv"
    calendars = read_data(path, cal_name, 'date')

    # select dates in each calendar df
    # for each monthly scrape, only keep the calendar data b/w the scraped date and the date of next scrape run
    # todo:wrap this into a func
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

    # put into one df
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
    # todo: imputation?

    # feature store
    log_time('Feature engineering - listing')
    df_list = fs_listing(df_data, output_all = True)

    log_time('Feature engineering - price')
    df_price = fs_price(df_data, monthly=True)

    log_time('Feature engineering - calendar')
    df_calendar = fs_calendar(df_data, output_all=True)

    log_time('Feature engineering - booked')
    df_booked = fs_booked(df_data, output_all=True)

    log_time('Feature engineering - review')
    df_review = fs_review(df_data, output_all=True)

    log_time('Feature engineering - location')
    df_location = fs_location(df_data, output_all=True)

    log_time('Feature engineering - host')
    df_host = fs_host(df_data, output_all=True)

    log_time('Feature engineering - time')
    df_time = fs_time(df_data, output_all=True)

    # upload
    engine = connect_my_db("secrets/db_string")

    log_time("Upload to psql - listing")
    upload_df(engine, df_list, "FS_LIST_MONTHLY")
    log_time("Upload to psql done - listing")

    log_time("Upload to psql - price")
    upload_df(engine, df_price, "FS_PRICE_MONTHLY")
    log_time("Upload to psql done - price")

    log_time("Upload to psql - calendar")
    upload_df(engine, df_calendar, "FS_CAL_MONTHLY")
    log_time("Upload to psql done - calendar")

    log_time("Upload to psql - booked")
    upload_df(engine, df_booked, "FS_BOOKED_MONTHLY")
    log_time("Upload to psql done - booked")

    log_time("Upload to psql - review")
    upload_df(engine, df_review, "FS_REVIEW_MONTHLY")
    log_time("Upload to psql done - review")

    log_time("Upload to psql - location")
    upload_df(engine, df_location, "FS_LOCATION_MONTHLY")
    log_time("Upload to psql done - location")

    log_time("Upload to psql - host")
    upload_df(engine, df_host, "FS_HOST_MONTHLY")
    log_time("Upload to psql done - host")

    log_time("Upload to psql - time")
    upload_df(engine, df_time, "FS_TIME_MONTHLY")
    log_time("Upload to psql done - time")






