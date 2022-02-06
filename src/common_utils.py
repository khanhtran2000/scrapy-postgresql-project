from bs4 import BeautifulSoup
from selenium import webdriver
import psycopg2
import sys
from datetime import datetime


# Run types
COVID_RT = "covid"
# Table names
COVID_TABLE_NAME = "covid_data_tbl"


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
        print("Error creating BeautifulSoup object : ", e)
        sys.exit(-2)

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
        print("> PostgreSQL connection is created.\n")
    except Exception as e:
        print("Error while connecting to PostgreSQL database : ", e)
        sys.exit(-2)

    return connection


def get_table_name(run_type: str):
    '''Return table name for a given run_type.
    :param type:
    '''
    try:
        if run_type == COVID_RT:
            table_name = COVID_TABLE_NAME
    except Exception as e:
        print(f"Error while getting table name for runtype {run_type} : ", e)
        sys.exit(-2)
    return table_name


#--------------- Build queries ---------------#

def build_insert_query(table_name: str, records_to_insert: tuple):
    '''Build query for INSERT statement
    :param records_to_insert:
    '''
    try:
        insert_query = f"""INSERT INTO {table_name} VALUES {records_to_insert}"""
    except Exception as e:
        print(f"Error while building insert query for table {table_name} : ", e)
        sys.exit(-2)

    return insert_query


def build_delete_query(table_name: str):
    pass


def build_select_query(table_name: str):
    pass

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
        print(f"Successfully ingested record {records_to_insert} into table {table_name}.\n")
    except Exception as e:
        print(f"Error while trying to insert records into table {table_name} : ", e)
        sys.exit(-2)


def run_delete_query(connection: psycopg2.extensions.connection, run_type: str):
    pass


def run_select_query(connection: psycopg2.extensions.connection, run_type: str):
    pass

#--------------- End of Run queries ---------------#