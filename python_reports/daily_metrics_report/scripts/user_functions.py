# importing required modules
# import tempfile
# import time as t

# import psycopg2
# import psycopg2.extras as extras
# import psycopg2.sql as sql
# from psycopg2 import InternalError, OperationalError, errorcodes
# from retry import retry
# from sqlalchemy import create_engine

from constants import snowflake_credentials

# *** MODIFY FUNCTION TO EXECUTE QUERY ON SNOWFLAKE AND FETCH RESPONSE IN DATAFRAME ***

# function for retrieving data from postgresql tables using psycopg2 copy_expert function
# @retry(tries=20, delay=30)
# def read_sql_tmpfile(query, connection):
#
#     with tempfile.TemporaryFile() as tmpfile:
#         # connecting to the DB and creating cursor object
#         global conn
#         global cur
#         global db_credentials
#         db_credentials = credentials['databases'][connection]
#         print("Connecting...")
#         conn = psycopg2.connect(**db_credentials)
#         cur = conn.cursor()
#         print("Connected Successfully..")
#         query_start_ts = t.time()
#
#         copy_sql = "COPY ({query}) TO STDOUT WITH CSV {head}".format(
#             query=query, head="HEADER"
#         )
#
#         cur.copy_expert(copy_sql, tmpfile)
#         tmpfile.seek(0)
#         df = pd.read_csv(tmpfile)
#
#         print('data copy completed')
#         print("closing connection")
#         conn.close()
#         seconds_taken = t.time() - query_start_ts
#         print('Query run time in seconds : {}'.format(seconds_taken))
#         return df

# auto adjust excel column width
def adjust_column_width(df, sheet_name, writer_name):
    for column in df:
            column_length = max(df[column].astype(
                str).map(len).max(), len(column))
            col_idx = df.columns.get_loc(column)
            writer_name.sheets[sheet_name].set_column(col_idx, col_idx, column_length)


# def __get_sqlalchemy_engine(connection):
#     """
#         This function creates a DB connection and returns DB engine using sqlalchemy library
#     Args:
#         connection (string): key for DB name present int the config file
#     Returns:
#         sql engine
#     """
#
#     database = credentials["databases"][connection]["database"]
#     username = credentials["databases"][connection]["user"]
#     host = credentials["databases"][connection]["host"]
#     password = credentials["databases"][connection]["password"]
#     port = credentials["databases"][connection]["port"]
#
#     engine_query = "postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}".format(
#         username=username, password=password, host=host, port=port, database=database
#     )
#     engine = create_engine(engine_query, echo=False)
#
#     return engine


def write_to_db(table_name, dataframe, connection, update_param, data_types=None):
    """
    This function reads the dataframe and appends the
    data to the respective table present on the database. This function will work if the db
    user has write permission on the database.
    Args:
        table_name (str): name of the table present on the database (new name in case table does not exist)
        dataframe: name of the dataframe which you want to push to the databases
        connection (str): database name to which you want to push the dataframe
        update_param (str): parameter required if we want to replace
        all the data or append in the table. The values could be "append" or "replace"
        data_types: To specifically define datatype of columns. Default value is None
        eg. write_to_db('test_table', df, 'redshift_db', 'replace', )
    """
    engine = __get_sqlalchemy_engine(connection)
    dataframe.to_sql(table_name, engine, if_exists=update_param,
                     chunksize=1000, index=False, dtype=data_types)
