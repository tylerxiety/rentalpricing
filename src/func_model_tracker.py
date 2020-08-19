# Here saves functions for model tracker

import plotly.graph_objs as go
import plotly.express as px
import pandas as pd
import datetime
from src.func_data_diagnosis import *
from src.func_data_engineering import *

def evaluate(df_predictions):
    """
    Take the input of a prediction data frame, and calculate an evaluation data frame
    :param dt_predictions: a data frame with columns COMPANY_ID, PRODUCT, DATE, PRED, ACTUAL
    :return: a data frame with columns METRIC, VALUE
    """

    # check data frame
    cols = ['ID', 'YEAR_MONTH', 'ACTUAL','PRED']
    check_dataframe(df_predictions, cols)

    # check if the input has NA, Null, Inf, None values
    df_inf = df_predictions.replace([np.inf, -np.inf], np.nan)
    if df_inf.isnull().any().any():
        sys.exit('Dataframe has null, na, or inf values')

    # calculate RMSE, MAE, MAPE, RMSLE at a global level and round to 2 decimal places
    row_number = df_predictions.shape[0]
    pred = df_predictions['PRED']
    actual = df_predictions['ACTUAL']

    rmse = np.sqrt(((actual - pred) ** 2).sum() / row_number).round(2)
    mae = ((actual - pred).abs().sum() / row_number).round(2)
    mape = (((actual - pred) / (actual+1)).abs().sum() / row_number).round(2)
    rmsle= np.sqrt(((np.log(actual + 1) - np.log(pred + 1)) ** 2).sum() / row_number).round(2)

    return pd.DataFrame({"METRIC": ["RMSE", "MAE", "MAPE", "RMSLE"],
                         "VALUE": [rmse, mae, mape, rmsle]})



def insert_evaluation(engine, df, table_name):
    """
    Insert evaluation records to existed table
    :param engine: the connection engine from func connect_my_db
    :param df: dataframe with columns 'EXPERIMENT_ID','SUBMISSION_ID','COMMENT','METRIC','VALUE'
    :param table: the name of the evaluation table in postgres
    """
    # insert records
    for i in range(df.shape[0]):
        values = "values({}, {}, '{}', '{}', '{}', {})".format(df.iloc[i]['EXPERIMENT_ID'],
                                                                     df.iloc[i]['SUBMISSION_ID'],
                                                                     df.iloc[i]['DATETIME'],
                                                                     df.iloc[i]['COMMENT'],
                                                                     df.iloc[i]['METRIC'],
                                                                     df.iloc[i]['VALUE'])

        insert_query = "INSERT INTO " + f'"{table_name}"' + values
        engine.execute(insert_query)


def update_submission_id(engine, table_name, experiment_id, date_time):
    """
    Update submission_id in evaluation table in postgres
    :param engine: the connection engine from func connect_my_db
    :param table: the name of the evaluation table in postgres
    :param experiment_id:
    :param date_time:
    :return:
    """
    # extract max_submission_id
    where1 = f'WHERE "EXPERIMENT_ID" = {experiment_id} AND "DATETIME" != "{date_time}"'
    #'where "EXPERIMENT_ID" = {} and "DATETIME" != '.format(experiment_id) + "'{}'".format(date_time)
    select_query = f'SELECT MAX("SUBMISSION_ID") AS max_submission_id FROM ' + f'"{table_name}" {where1}'
    #"select max(" + '"SUBMISSION_ID") as max_submission_id from' + f'"{table}" ' + where_pre

    df = pd.read_sql_query(select_query, con=engine)

    if df.iloc[0]['max_submission_id'] is None:
        new_submission_id = 1
    else:
        new_submission_id = df.iloc[0]['max_submission_id'] + 1
        # update the new_submission_id
        where2 = f'WHERE "EXPERIMENT_ID"={experiment_id} AND "DATETIME"={date_time}'
        #'WHERE "EXPERIMENT_ID" = {} AND "DATETIME" = '.format(experiment_id) + "'{}'".format(date_time)
        update_query = f'UPDATE "{table_name}" '+ f'SET "SUBMISSION_ID" = {new_submission_id} {where2}'
        #"UPDATE " + f'"{table}" '+"SET "+'"SUBMISSION_ID" = {} '.format(new_submission_id)+where2
        engine.execute(update_query)

    return new_submission_id

def submit(df_evaluation, dict_params):
    """
    Submit the evaluation data frame (created from make_metrics()) to MODEL_TRACKER table on PostgreSQL
    :param df_evaluation: a data frame with columns METRIC, VALUE
    :param dict_params: a dictionary of experiment_id, user_name, comment. Comment specifies the model settings:
    model type, level, key model parameters, and etc
    :return: a submission ID
    """

    # check data frame
    cols = ['METRIC', 'VALUE']
    check_dataframe(df_evaluation, cols)

    # check dictionary
    keys = ['experiment_id', 'comment']
    check_dictionary(dict_params, keys)

    # generate an initial submission ID
    dict_params['submission_ID'] = 1

    # generate a datetime variable with datetime.datetime.now()
    now = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    dict_params['datetime'] = now

    # make a data frame with column names EXPERIMENT_ID, SUBMISSION_ID, DATETIME, COMMENT, METRIC, VALUE
    df_params = pd.DataFrame(dict_params, index=[0])
    df_params.columns = df_params.columns.upper()

    df_evaluation['EXPERIMENT_ID'] = dict_params['experiment_id']
    df_new = pd.merge(df_params, df_evaluation, how='left', on=['EXPERIMENT_ID', 'EXPERIMENT_ID'])

    df_new = df_new[['EXPERIMENT_ID', 'SUBMISSION_ID', 'DATETIME', 'COMMENT', 'METRIC', 'VALUE']]

    # check if table is existing in PostgreSQL, if not, upload; else: insert
    engine = connect_my_db("secrets/db_string")

    if not check_table(engine, 'MODEL_TRACKER'):
        upload_df(engine, df_new, 'MODEL_TRACKER')
    else:
        insert_evaluation(engine, df_new, 'MODEL_TRACKER')

    # if the upload is successful, return the submission ID
    submission_ID = update_submission_id(engine, 'MODEL_TRACKER', dict_params['experiment_id'], now)

    return submission_ID


def check_table(engine, table_name):
    # Read
    select_query = "select * from " + f'"{table_name}"'
    try:
        result_set = engine.execute(select_query).fetchall()
        return True
    except:
        return False



def get_submissions(experiment_id):
    """
    Read the submission information, based on dict_params from the MODEL_TRACKER table on PostgreSQL
    :experiment_id: experiment_id to get
    :return: a data frame with ONLY the submission information for the experiment_id
    :return: a one-row data frame to represent the best RMSLE submission
    """

    # check in the MODEL_TRACKER table whether experiment_id exists
    engine = connect_my_db("secrets/db_string")

    df_subset = get_subset_from_db(engine, experiment_id, "MODEL_TRACKER")
    if df_subset.empty:
        sys.exit('Experiment_id does not exist.')

    else:
        # get a one-row data frame to represent the best RMSLE submission
        rmsle_best = df_subset[df_subset.METRIC.str.upper() == 'RMSLE']['VALUE'].min()
        df_rmsle = df_subset[(df_subset.METRIC.str.upper() == 'RMSLE') & (df_subset.VALUE == rmsle_best)]

    # dispose engine
    engine.dispose()

    return df_subset,df_rmsle


def get_subset_from_db(engine, params, table):
    """
    Get a subset from database
    :param params: a dictionary with keys as experiment_id, or experiment_id and submission_id
    :param table: the table name in the database
    :return: a data frame of queried subset data
    """

    # connect database to check in the table whether value combo exists


    if isinstance(params, dict):
        experiment_id = params['experiment_id']
        submission_id = params['submission_id']
        where_clause = f'WHERE "EXPERIMENT_ID" = {experiment_id} and "SUBMISSION_ID"={submission_id}'
    else:
        where_clause = f'WHERE "EXPERIMENT_ID" = {params}'

    select_query = 'SELECT * FROM ' + f'"{table}" {where_clause} ORDER BY "SUBMISSION_ID";'
    #"SELECT * FROM " + f'"{table}" ' + where_clause + 'ORDER BY "SUBMISSION_ID";'

    df_subset = pd.read_sql_query(select_query, con=engine)

    return df_subset


def del_submissions(dict_params):
    """
    Delete a row or rows from the MODEL_TRACKER table on PostgreSQL, based on the dict_params
    :param dict_params: a dictionary of experiment_id, submission_id (only one id at a time when deleting)
    :return: a message "experiment_id: XXX, submission_id: XXX is deleted" then print the deleted subset
    """

    # check dictionary and the required keys
    keys = ['experiment_id', 'submission_id']
    check_dictionary(dict_params, keys)

    # check in the MODEL_TRACKER table whether experiment_id and submission_id combo exists
    engine = connect_my_db("secrets/db_string")

    df_subset = get_subset_from_db(engine, dict_params, "MODEL_TRACKER")
    if df_subset.empty:
        sys.exit('Experiment_id and submission_id combo not exist.')

    else:
        # delete the rows
        del_subset_from_db(engine, dict_params, "MODEL_TRACKER")

    print(f'Experiment_id: {dict_params["experiment_id"]}, submission_id: {dict_params["submission_id"]} is deleted')

    # dispose engine
    engine.dispose()

    return df_subset


def del_subset_from_db(engine, dict_params, table):
    """
    Delete the subset data in the database
    :param dict_params: a dictionary with keys as experiment_id and user_name, or experiment_id and submission_id
    :param table: the table name in the database
    :return: delete the subset and return nothing
    """

    # connect database to check in the table whether value combo exists
    experiment_id = dict_params['experiment_id']
    submission_id = dict_params['submission_id']
    delete_query = 'DELETE FROM ' + f'"{table}" WHERE "EXPERIMENT_ID" = {experiment_id} AND "SUBMISSION_ID" = {submission_id}'

    engine.execute(delete_query)

    return


def plot_submissions(df_submissions):
    """
    Create several plots based on df_submissions, a subset of the MODEL_TRACKER table
    :param df_submissions: a data frame subset of the MODEL_TRACKER table
    :return: a dictionary of plots at experiment level, and represented by the different metrics
    """
    columns = ['EXPERIMENT_ID', 'SUBMISSION_ID', 'DATETIME', 'COMMENT', 'METRIC', 'VALUE']
    experiments = list(df_submissions['EXPERIMENT_ID'].unique())
    metrics = list(df_submissions['METRIC'].unique())

    # check dataframe
    check_dataframe(df_submissions, columns)

    # plot for every experiment
    for exp in experiments:
        # dic_result: save the final plots for every experiment
        dic_result = {}

        # dic_fig: save different plots for every metric
        dic_fig = {}
        for metr in metrics:
            df_use = df_submissions[(df_submissions['EXPERIMENT_ID'] == exp) & (df_submissions['METRIC'] == metr)]
            # for users who have multiple metric values, keep the minimum value
            # df_use = df_use.groupby(['USER', 'METRIC']).apply(lambda x: x['VALUE'].min()).to_frame()
            df_use['VALUE'] = df_use[0]
            df_use = df_use.reset_index()
            # bar chart plot object for every user
            fig = px.bar(df_use, x='USER', y='VALUE')
            dic_fig[metr] = fig
            dic_ranking = dict([('plot_ranking', dic_fig)])

        # # dic_user: save different plots for users
        # dic_user = {}
        # for i in user:
        #     # dic_user_fig: save different metric plots for every user
        #     dic_user_fig = {}
        #     for j in metric:
        #         df_use = df_submissions[(df_submissions['EXPERIMENT_ID'] == each) & (df_submissions['USER'] == i) & (
        #                 df_submissions['METRIC'] == j)]
        #         # line chart plot object for every user
        #         fig = go.Figure(data=go.Scatter(x=df_use['SUBMISSION_ID'], y=df_use['VALUE'], mode='lines+markers',
        #                                         text=df_use['COMMENT']))
        #         dic_user_fig[j] = fig
        #     dic_user[i] = dic_user_fig
        #
        # dic_submission_user = dict([('plot_submission_by_user', dic_user)])

        # dic_submission: save different plots for every metric
        dic_submission = {}
        for metr in metrics:
            df_use = df_submissions[(df_submissions['EXPERIMENT_ID'] == exp) & (df_submissions['METRIC'] == metr)]
            # line chart plot object for every metric
            fig = px.line(df_use, x='SUBMISSION_ID', y='VALUE', color='USER', text='COMMENT')
            fig.update_traces(mode="markers+lines", hovertemplate=None)
            dic_submission[metr] = fig
            dic_multiple_submission = dict([('plot_submission', dic_submission)])

        #dic_a = dict(dic_user_ranking, **dic_submission_user)
        dic_all = dict(dic_ranking, **dic_multiple_submission)

        dic_result = dict([(exp, dic_all)])

        return dic_result