import pandas as pd
from config import DB_DETAILS
from mysql import connector as mc
from mysql.connector import errorcode as ec
import psycopg2 as psy


def get_tables(path):

    tables=pd.read_csv(path,sep=':')
    return tables[tables['to_be_loaded']=='yes']

def load_db_details(env):
    return DB_DETAILS[env]

def get_mysql_connection(db_host,db_name,db_user,db_pass):
    connection=None
    try:
        connection = mc.connect(user=db_user, password=db_pass, host=db_host,database=db_name)
    except mc.Error as error:
        if error.errno==ec.ER_ACCESS_DENIED_ERROR:
            print("Invalid Credentials")
        else:
            print(error)

    return connection


def get_postgres_connection(db_host, db_name, db_user, db_pass):
    connection = None
    try:
        connection = psy.connect(user=db_user, password=db_pass, host=db_host, dbname=db_name)
    except psy.OperationalError as error:
        if error.pgcode == '28P01':  # SQLSTATE for invalid authentication (wrong username/password)
            print("Invalid credentials")
        else:
            print(f"Operational error: {error}")
    except psy.Error as error:
        print(f"Database error: {error}")
    return connection


def get_connection(db_type,db_host,db_name,db_user,db_pass):
    connection=None
    if db_type=='mysql':
        connection=get_mysql_connection(db_host=db_host,db_name=db_name,db_user=db_user,db_pass=db_pass)
    if db_type=='postgres':
        connection=get_postgres_connection(db_host=db_host,db_name=db_name,db_user=db_user,db_pass=db_pass)

    return connection










