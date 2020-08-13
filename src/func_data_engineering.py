# Here saves data engineering functions

from datetime import datetime
import yaml
from sqlalchemy import create_engine
import time

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


def fs_upload(engine, df, table_name):
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

