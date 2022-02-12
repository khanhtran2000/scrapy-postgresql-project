from bs4 import BeautifulSoup
from selenium import webdriver
import psycopg2
import sys
import os
import logging
from logging.handlers import WatchedFileHandler
from datetime import datetime


logger = None
# Run types
COVID_RT = "covid"
# Table names
COVID_TABLE_NAME = "covid_data_tbl"


#--------------- Logging utilities ---------------#

def create_log(path=None):
    '''Create a log file with default level at INFO.
    '''
    global logger

    if logger is not None:
        return logger

    if path is None:
        path = get_log_file_path()

    handler = WatchedFileHandler(os.environ.get("LOGFILE", path))
    formatter = logging.Formatter(logging.BASIC_FORMAT)
    handler.setFormatter(formatter)
    logger = logging.getLogger("PipelineLog")
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    return logger


def get_abs_root_path():
    '''Get absoulate root path.
    '''
    return os.path.abspath(os.path.dirname(__file__))


def get_log_file_path():
    '''Get log file path. Create on if not exist.
    '''
    logs_folder_path = os.path.join(get_abs_root_path(), r"../logs/")
    if not os.path.isdir(logs_folder_path):
        os.mkdir(logs_folder_path)  # create path if not found

    log_file_path = os.path.join(logs_folder_path, r"pipeline.log")
    if not os.path.isfile(log_file_path):
        open(log_file_path, 'w').close()  # create file if not found

    return log_file_path


def get_logger():
    '''Create logger.
    '''
    global logger
    if logger is None:
        logger = create_log()
    return logger

#--------------- End of Logging utilities ---------------#


def create_soup(url: str, chrome_path: str) -> BeautifulSoup:
    '''Return a BeautifulSoup object.
    :param url: the url that we want to scrape
    :param chrome_path: path to chromedriver
    '''
    try:
        options = webdriver.ChromeOptions()
        options.add_argument("headless")
        driver = webdriver.Chrome(executable_path=chrome_path, chrome_options=options)
        driver.get(url)
        html = driver.page_source
        soup = BeautifulSoup(html,'lxml')
    except Exception as e:
        get_logger().error(f"Error while creating BeautifulSoup object for url {url}: "  + " Error: " + str(sys.exc_info()[0]))

    return soup


def get_current_date() -> str:
    '''Return current date as a string.
    '''
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return now


def create_postgres_connection(user: str, password: str, host: str, port: str, database: str) -> psycopg2.extensions.connection:
    '''Create a connection to PostgresSQL database
    :param user:
    :param password:
    :param host:
    :param port:
    :param database:
    '''
    try:
        connection = psycopg2.connect(user=user,
                                      password=password,
                                      host=host,
                                      port=port,
                                      database=database)
        get_logger().info("> PostgreSQL connection is created.\n")
    except:
        get_logger().error("Error while connecting to PostgreSQL database : " + " Error: " + str(sys.exc_info()[0]))

    return connection


def get_table_name(run_type: str):
    '''Return table name for a given run_type.
    :param type:
    '''
    try:
        if run_type == COVID_RT:
            table_name = COVID_TABLE_NAME
    except:
        get_logger().error(f"Error while getting table name for runtype {run_type} : " + " Error: " + str(sys.exc_info()[0]))

    return table_name


#--------------- Build queries ---------------#

def build_insert_query(table_name: str, records_to_insert: tuple):
    '''Build query for INSERT statement
    :param table_name: name of the table
    :param records_to_insert: records to insert into the given table
    '''
    try:
        insert_query = f"""INSERT INTO {table_name} VALUES {records_to_insert}"""
    except:
        get_logger().error(f"Error while building insert query for table {table_name} : " + " Error: " + str(sys.exc_info()[0]))

    return insert_query


def build_delete_query(table_name: str):
    pass


def build_select_query(table_name: str, fields: str):
    '''Build query for SELECT statement
    :param table_name: name of the table
    :param fields: fields want to be selected from the given table
    '''
    try:
        select_query = f"SELECT {fields} FROM {table_name}"
    except:
        get_logger().error(f"Error while building select query for table {table_name} : " + " Error: " + str(sys.exc_info()[0]))


#--------------- End of Build queries ---------------#


#--------------- Run queries ---------------#

def run_insert_query(connection: psycopg2.extensions.connection, run_type: str, records_to_insert: tuple):
    '''Execute the insert records query into PostgreSQL table.
    :param connection:
    :param table_name:
    :param records_to_insert:
    '''
    cursor = connection.cursor()
    table_name = get_table_name(run_type=run_type)
    insert_query = build_insert_query(table_name=table_name, records_to_insert=records_to_insert)

    try:
        cursor.execute(insert_query, records_to_insert)
        connection.commit()
        get_logger().info(f"Successfully ingested record {records_to_insert} into table {table_name}.\n")
    except:
        get_logger().error(f"Error while trying to insert records into table {table_name} : " + " Error: " + str(sys.exc_info()[0]))


def run_delete_query(connection: psycopg2.extensions.connection, run_type: str):
    pass


def run_select_query(connection: psycopg2.extensions.connection, run_type: str):
    pass

#--------------- End of Run queries ---------------#