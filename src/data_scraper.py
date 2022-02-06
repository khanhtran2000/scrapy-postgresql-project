from dotenv import load_dotenv
import os
import sys

import common_utils as cu


load_dotenv()
class DataScraper():
    def __init__(self):
        self.logger = cu.create_log()
        self.url = os.getenv("URL")
        self.chrome_path = os.getenv("CHROMEPATH")
    
    def scrape_data(self):
        pass


class CovidScraper(DataScraper):
    def scrape_data(self):
        '''Return clean records to ingest into PostgreSQL database.
        :param url:
        :param chrome_path:
        '''
        try:
            # Get the soup
            soup = cu.create_soup(url=self.url, chrome_path=self.chrome_path)
            # Get the raw values
            rows = soup.find_all("ul", {"class":"list-tinhthanh", "id":"list-tinhthanh"})
            values = rows[0].find_all("div", {"class":"td"})
            raw_values = [value.text for value in values]
            self.logger.info("> Records are scraped from the URl.\n")
        except:
            self.logger.error(f"Error while scraping data from URL {self.url} : " + " Error: " + str(sys.exc_info()[0]))

        
        return raw_values


class OtherScraper(DataScraper):
    pass
