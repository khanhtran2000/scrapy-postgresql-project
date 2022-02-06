import data_ingester as di
import common_utils as cu


def main(run_type: str):
    if run_type == cu.COVID_RT:
        ingester = di.CovidIngester()
        ingester.ingest()
    else:
        pass
    
    print("> PostgreSQL connection is closed.")


if __name__ == "__main__":
    main(run_type="covid")