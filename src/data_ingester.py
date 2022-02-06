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
        except Exception as e:
            print("Error while reading the database user : ", e)
            sys.exit(-2)

    
    def get_password(self):
        '''Return database password.
        '''
        try:
            return self.password
        except Exception as e:
            print("Error while reading the database password : ", e)
            sys.exit(-2)

    def get_host(self):
        '''Return database host.
        '''
        try:
            return self.host
        except Exception as e:
            print("Error while reading the database host : ", e)
            sys.exit(-2)

    def get_port(self):
        '''Return database port.
        '''
        try:
            return self.port
        except Exception as e:
            print("Error while reading the database port : ", e)
            sys.exit(-2)

    def get_database(self):
        '''Return database name.
        '''
        try:
            return self.database
        except Exception as e:
            print("Error while reading the database name : ", e)
            sys.exit(-2)
    
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
    def ingest(self):
        '''Update new scraped covid data into the table.
        '''
        # Connect
        connection = self.connect_to_postgres()
        # Scrape
        scraper = ds.CovidScraper()
        raw_records = scraper.scrape_data()
        # Clean
        cleaner = dc.CovidCleaner()
        clean_records = cleaner.get_clean_records(raw_records=raw_records)
        # Ingest
        for re in clean_records:
            cu.run_insert_query(connection=connection, run_type=cu.COVID_RT, records_to_insert=re)
        
        connection.close()


class OtherIngester(DataIngester):
    pass