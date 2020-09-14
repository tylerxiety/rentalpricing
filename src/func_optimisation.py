# Here saves functions for optimisation

import numpy as np
import pandas as pd
import time
from src.func_data_diagnosis import *
from src.func_model_api import *
import plotly.express as px



def create_multiple_prices(df,range_max=2, num_prices=11):
    """
    Take the input dataframe and return multiple price points and add them in the PRICE column
    :param df: a dataframe with columns BASE_PRICE and other features.
    :param range_max: the end point of the price range.
    :param num_prices: the number of prices created from the range.
    :return: a data frame with the same columns as the input plus PRICE
    """

    df_prices = pd.DataFrame()

    df_cp = df.copy()
    for i in np.linspace(1, range_max, num_prices, endpoint=True):
        df_cp['PRICE'] = round(i * df_cp['BASE_PRICE'])
        df_cp['PRICE_RATE'] = i
        df_prices = df_prices.append(df_cp, ignore_index=True)

    return df_prices


def get_demand_curve(df):
    """
    Take the input dataframe and return the output of demand curves(s)
    :param df: a data frame with the columns same to the inputs for Model API, but with multiple prices.
    :return: a data frame with columns ID, YEAR_MONTH, PRICE, BOOKED, REVENUE.
    The higher the price, the lower the demand, while BOOKED can be the same for two consecutive prices
    """
    # Model API, get predictions

    df_cp = df.copy()
    # extract PRICE_RATE
    df_rate = df_cp[['ID', 'YEAR_MONTH', 'PRICE', 'PRICE_RATE']]

    # change col name, todo: rm after changed model
    df_cp = df_cp.rename(columns={'PRICE': 'TXN_PRICE'})

    # drop PRICE_RATE for prediction
    df_cp = df_cp.drop('PRICE_RATE', axis=1)

    # get prediction
    df_pred = global_model_api(df_cp)
    # change col name, todo: rm after changed model
    df_pred = df_pred.rename(columns={'TXN_PRICE': 'PRICE'})

    # put back PRICE_RATE
    df_pred = df_pred.merge(df_rate, on = ['ID','YEAR_MONTH','PRICE'])
    # Monotonic transformation
    df_demand_curve = transform_monotonic(df_pred)

    # add the REVENUE column
    df_demand_curve['REVENUE'] = np.round(df_demand_curve.PRICE * df_demand_curve.BOOKED)

    # ensure data type if needed
    # df_demand_curve = change_data_types(df_demand_curve)

    return df_demand_curve


def transform_monotonic(df):
    """
    Take the df and return the monotonic demand curve(s)
    :param df: a data frame with columns ID, YEAR_MONTH, PRICE, BOOKED.
    :return: a data frame with the same columns as the input. The higher the PRICE is for the same ID,
    the lower the BOOKED, while BOOKED can be the same for two consecutive PRICEs
    """

    # make a copy
    df_cp = df.copy()

    # initialise output df
    df_monotonic = pd.DataFrame()

    for id in df_cp.ID.unique():
        df_id = df_cp[df_cp.ID == id].set_index('PRICE')
        df_id['BOOKED'] = np.minimum.accumulate(df_id['BOOKED'])
        df_id = df_id.reset_index()
        df_monotonic = df_monotonic.append(df_id, ignore_index=True)

    return df_monotonic

def create_objective_col(df_demand_curve, cost = 0):
    """
    Take the input df_demand_curve and return the new objective column
    :param df_demand_curve: a data frame, the output from get_demand_curve
    :return: a data frame with the same columns as df_demand_curve, but with OBJ column
    """
    df_demand_curve['OBJ'] = round(df_demand_curve['REVENUE'] + df_demand_curve['BOOKED'] * cost)

    return df_demand_curve


def optimise_brute_force(df):
    """
    Take the input df (output from get_demand_curve) and return the subset of df, where each row in the subset
    represents the best prices
    :param df: a data frame with columns, ID, YEAR_MONTH, PRICE, BOOKED, OBJ
    :return: a subset data frame with the same columns
    """

    # get the index with max OBJ
    idx = df.groupby(['ID'])['OBJ'].transform(max) == df.OBJ
    df_best = df[idx]

    return df_best

def diagnose_output(df_optimised):
    """
    Take the two input dfs and return a set of diagnosis
    :param df_demand_curve: a data frame, the output from get_demand_curve
    :param df_optimised: a data frame, the output from an optimsation function, e.g. optimise_brute_force
    :return: a list of diagnosis, including:
    1) A  histograms of the Distribution of the price rate
    2) A Data Frame of a ranked list of properties, based on the chosen price rate
    """

    # make a copy
    df_cp = df_optimised.copy()

    # # create COMBO, MARGIN_RATE and PRICE_COST_RATIO
    # df_op['COMBO'] = df_op.ID.astype(str)+'_'+df_op.YEAR_
    # df_op['MARGIN_RATE']=round(df_op.PRICE/df_op.COST-1,2)
    # df_op['PRICE_COST_RATIO']=round(df_op.PRICE/df_op.COST,2)
    # dist_hist = {}
    #
    # # create a dict of plots
    # # for each id
    # for id in df_cp.ID.unique():
    #     df_id = df_cp[df_cp.ID==id]
    #     plot_id=px.histogram(df_id[['PRICE_RATE']],
    #              x='PRICE_RATE',
    #              histnorm='probability',
    #             labels={'count':'Distribution'},
    #              title='{} plot'.format(id))
    #
    #     dist_hist[id] = plot_id

    # for overall
    plot=px.histogram(df_cp[['PRICE_RATE']]
                      ,x='PRICE_RATE'
                      ,histnorm='probability'
                      ,title='PRICE_RATE plot'
                      ,nbins=10)



    # create a Data Frame of a ranked list of listings
    df_rank = df_cp[['ID','PRICE_RATE']]
    df_rank['RANKING']=df_rank['PRICE_RATE'].rank(method='min', ascending=False)
    df_rank = df_rank.sort_values(by='RANKING')
    return plot, df_rank

def view_demand_curve(df):
    """
    Take the dataframe (output from get_demand_curve) and return a dictionary of plots of demand curves
    Each key is property ID
    :param df: a data frame with columns, COMPANY_ID, PRODUCT, DATE, PRICE, COST, UNIT, PROFIT
    :return: a dictionary of plots, only need to plot PRICE vs. UNIT
    """
    df_cp = df.copy()
    dist_dc = {}
    for id in df_cp.ID.unique():
        df_id = df_cp[df_cp.ID == id]
        plot=px.line(df_id[['BOOKED','PRICE']],
                     x='PRICE', y='BOOKED',
                     title=f'Listing {id} Plot')
        dist_dc[id]= plot

    return dist_dc