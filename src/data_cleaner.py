import sys
import psycopg2
import os

import common_utils as cu


class DataCleaner():
    def __init__(self):
        self.logger = cu.create_log()
        self.rt = cu.COVID_RT
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

    def clean_raw_data(self):
        pass


class CovidCleaner(DataCleaner):
    def clean_raw_data(self, raw_records: list):
        '''Clean given raw scraped data.
        :param raw_records: list of raw scraped records.
        '''
        try:
            # A little data cleaning
            clean_records = []
            for value in raw_records:
                if value != "-":
                    clean_records.append(value)
                elif value == "-":
                    clean_records.append("0")
            cities = [clean_records[i] for i in range(0, len(clean_records), 7)]
            actives = [int(clean_records[i].replace(".", "")) for i in range(1, len(clean_records), 7)]
            actives_today = [int(clean_records[i].replace(".", "").replace("+", "")) for i in range(2, len(clean_records), 7)]
            deaths = [int(clean_records[i].replace(".", "")) for i in range(4, len(clean_records), 7)]
            deaths_today = [int(clean_records[i].replace(".", "")) for i in range(5, len(clean_records), 7)]
            self.logger.info("> Records are cleaned.\n")
        except:
            self.logger.error(f"Error while cleaning raw data : " + " Error: " + str(sys.exc_info()[0]))

        return cities, actives, actives_today, deaths, deaths_today

    def create_ids_and_dates(self, raw_records: list):
        '''Create IDs and dates for the clean records.
        :param raw_records: list of raw scraped records.
        '''
        try:
            # Get date of the scrape
            dates = [cu.get_current_date() for i in range(len(raw_records))]
            # Create a list of IDs for the records
            max_id = cu.run_select_query(connection=self.connect_to_postgres(), run_type=self.rt, fields="MAX(id)")[0][0]

            ids = [i for i in range(max_id+1, max_id+len(raw_records)+1)]
            self.logger.info("> IDs and dates are created for clean records.\n")
        except:
            self.logger.error("Error while creating IDs and dates for clean records : " + " Error: " + str(sys.exc_info()[0]))

        return ids, dates

    def get_clean_records(self, raw_records: list):
        '''Return clean records for data ingestion.
        :param raw_records: list of raw scraped records.
        '''
        cities, actives, actives_today, deaths, deaths_today = self.clean_raw_data(raw_records=raw_records)
        ids, dates = self.create_ids_and_dates(raw_records=raw_records)
        
        try:
            clean_records = list(zip(ids, dates, cities, actives, actives_today, deaths, deaths_today))
            self.logger.info("> Records are ready for data ingestion.\n")
        except Exception as e:
            self.logger.error("Error while finalizing records for data ingestion : " + " Error: " + str(sys.exc_info()[0]))

        return clean_records


class OtherCleaner(DataCleaner):
    pass