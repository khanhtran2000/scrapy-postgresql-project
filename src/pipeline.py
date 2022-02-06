import getopt
import sys

import data_scraper as ds
import data_cleaner as dc
import data_ingester as di
import common_utils as cu


class Pipeline():
    def __init__(self):
        self.logger = cu.create_log()

    def execute_flow(self):
        pass


class CovidPipeline(Pipeline):
    def execute_flow(self):
        # Scrape
        scraper = ds.CovidScraper()
        raw_records = scraper.scrape_data()
        # Clean
        cleaner = dc.CovidCleaner()
        clean_records = cleaner.get_clean_records(raw_records=raw_records)
        # Ingest
        ingester = di.CovidIngester()
        ingester.ingest(clean_records=clean_records)


def main(run_type: str):
    if run_type == cu.COVID_RT:
        CovidPipeline().execute_flow()
    else:
        pass


if __name__ == "__main__":
    rt = cu.COVID_RT
    valid_args = True
    valid_rt = (cu.COVID_RT)
    logger = cu.create_log()
    try:
        opts, args = getopt.getopt(sys.argv[1:], "e", ["run_type="])
    except getopt.GetoptError as err:
        logger.error(err)
        sys.exit(2)
    for o, a in opts:
        if o == "--run_type":
            rt = a
    if rt.lower() not in valid_rt:
        logger.error(f"Invalid run_type arg: \'{rt}\'. Valid run_types are: {valid_rt}")
        valid_args = False
    if not valid_args:
        sys.exit(2)

    main(run_type=rt)