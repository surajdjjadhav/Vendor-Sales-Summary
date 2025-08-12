from src.logger import config_logger
import pandas as pd
from src.components.data_injetion import VendorSalesSummary, DataIngestion
from src.components.data_injetion import DataPusher
from src.components.datatransformer import VendorDataTransformer
from src.components.datacleaner import VendorDataCleaner
from src.connection.MySqlClient import MySqlClient
from src.components.schema_manager import SchemaManager, VendorSalesSummaryTableCreator
import time
from src.exception import MyException

logger = config_logger()

client = MySqlClient(
    user="suraj",
    password="YourActualPasswordHere",
    db="SURAJ",
    host="localhost",
    port=3306,
)


class VendorPerformancePipeline:
    def __init__(self, user, password, database: str, host, table_name: str):
        self.user = user
        self.database = database
        self.password = password
        self.host = host
        self.table_name = table_name

    def run(self):
        try:
            logger.info("Vendor performance pipeline started.")

            # Step 1: Connect to database
            client = MySqlClient(self.user, self.password, self.database, self.host)
            raw_conn = client.engine(with_db=True)

            # Step 2: Load raw data
            sales_summary_loader = VendorSalesSummary(raw_conn)
            raw_df = sales_summary_loader.load_summary()

            # Step 3: Clean the data
            cleaner = VendorDataCleaner(df=raw_df)
            clean_df = cleaner.clean()

            pusher = DataPusher(
                raw_conn,
                self.database,
                table_name="clean_data",
                if_exists="replace",
            )
            pusher.push_data(clean_df)

            # Step 4: Transform the data
            transformer = VendorDataTransformer(df=clean_df)
            final_df = transformer.transform()

            # Step 5: Create the table
            table_creator = VendorSalesSummaryTableCreator(raw_conn)
            table_creator.create_table(self.table_name)

            # Step 6: Push final data to the table
            data_pusher = DataPusher(
                conn=raw_conn,
                database=self.database,
                table_name=self.table_name,
                if_exists="replace",
            )
            data_pusher.push_data(final_df)

            logger.info("Vendor performance pipeline completed successfully.")
        except Exception as e:
            raise MyException(e)


def data_inject(data_dir, user, password, database, host):
    try:
        # --------------------- data base creation done---------------------------------------------------------------
        logger.info("data injection started")
        client = MySqlClient(user, password, database, host)
        raw_conn = client.engine(with_db=False)
        schema = SchemaManager(raw_conn, database)
        schema.Create_Database()
        # --------------------- data injection into data base ---------------------------------------------------------------
        conn = MySqlClient(user, password, database, host).engine(with_db=True)
        ingestion = DataIngestion(data_dir, conn, database)
        ingestion.ingest_all_files()
        logger.info("data injection done sucessfuly")
    except Exception as e:
        raise MyException(e)


# ata_dir_path = "src/components/data/data"

# data_inject(data_dir=data_dir_path  , user="suraj" , password="Suraj@2510" , database="sales_data" , host="localhost")


def main(data_dir_path, user, password, database, host, table_name_vendor_sumary):
    try:
        start_time = time.time()

        data_inject(data_dir_path, user, password, database, host)

        logger.info("Starting vendor performance pipeline...")
        vendor = VendorPerformancePipeline(
            user, password, database, host, table_name_vendor_sumary
        )
        vendor.run()
        end_time = time.time()
        total_time = (end_time - start_time) / 60

        logger.info(f"total time require for run ths entire pipeline {total_time}")
        logger.info("Pipeline finished successfully.")

    except Exception as e:
        raise MyException(e)


if __name__ == "__main__":
    data_dir_path = "src/components/data/data"
    main(
        data_dir_path=data_dir_path,
        user="suraj",
        password="Suraj@2510",
        database="sales_data",
        host="localhost",
        table_name_vendor_sumary="VendorSalesSummay",
    )
