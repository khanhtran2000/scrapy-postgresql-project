from multiprocessing import connection
from dotenv import load_dotenv
import os
import sys

import data_scraper as ds
import data_cleaner as dc
import common_utils as cu


load_dotenv()

class DataIngester():
    def __init__(self):
        self.logger = cu.create_log()
        self.user = os.getenv("DB_USER")
        self.password = os.getenv("PASSWORD")
        self.host = os.getenv("HOST")
        self.port = os.getenv("PORT")
        self.database = os.getenv("DATABASE")
    
    def get_user(self):
        '''Return database user.
        '''
        try:
            return self.user
        except:
            self.logger.error("Error while reading the database user : " + " Error: " + str(sys.exc_info()[0]))
    
    def get_password(self):
        '''Return database password.
        '''
        try:
            return self.password
        except:
            self.logger.error("Error while reading the database password : " + " Error: " + str(sys.exc_info()[0]))

    def get_host(self):
        '''Return database host.
        '''
        try:
            return self.host
        except:
            self.logger.error("Error while reading the database host : " + " Error: " + str(sys.exc_info()[0]))

    def get_port(self):
        '''Return database port.
        '''
        try:
            return self.port
        except:
            self.logger.error("Error while reading the database port : " + " Error: " + str(sys.exc_info()[0]))

    def get_database(self):
        '''Return database name.
        '''
        try:
            return self.database
        except:
            self.logger.error("Error while reading the database name : " + " Error: " + str(sys.exc_info()[0]))
    
    def connect_to_postgres(self):
        '''Create a connection to the given database.
        '''
        connection = cu.create_postgres_connection(user=self.get_user(),
                                                password=self.get_password(),
                                                host=self.get_host(),
                                                port=self.get_port(),
                                                database=self.get_database())

        return connection

    def ingest(self):
        pass


class CovidIngester(DataIngester):
    def ingest(self, clean_records: list):
        '''Update new scraped covid data into the table.
        '''
        # Connect
        connection = self.connect_to_postgres()
        # Ingest
        for re in clean_records:
            cu.run_insert_query(connection=connection, run_type=cu.COVID_RT, records_to_insert=re)
        
        connection.close()

        self.logger.info("> PostgreSQL connection is closed.")


class OtherIngester(DataIngester):
    pass