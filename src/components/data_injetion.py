import os
import time
import pandas as pd
from src.logger import config_logger
from src.exception import MyException

logger = config_logger()
# from src.connection.SQLiteClient import DataPusher
from src.connection.MySqlClient import DataPusher, MySqlClient
from pandas import DataFrame


class DataIngestion:
    def __init__(self, data_dir: str, conn, database, chunksize: int = 100000):
        self.conn = conn
        self.data_dir = data_dir
        self.database = database
        self.chunksize = chunksize

    def ingest_all_files(self):
        """
        this method used to load data from loacl and push this data to database

        """
        try:
            for file in os.listdir(self.data_dir):
                if file.endswith(".csv"):
                    self.ingest_single_file(file)
        except Exception as e:
            raise MyException(e)

    def ingest_single_file(self, file):
        try:
            file_path = os.path.join(self.data_dir, file)
            table_name = file[:-4]
            logger.info(f"Starting ingestion for file: {file}")
            start_time = time.time()

            chunk_iter = pd.read_csv(file_path, chunksize=self.chunksize)
            logger.info(f"data reading sucessffuly done for file {file}")

            for chunk in chunk_iter:
                pusher = DataPusher(
                    self.conn,
                    self.database,
                    table_name,
                    if_exists="append",
                )
                pusher.push_data(chunk)
                logger.info(f"Pushed :: {len(chunk)} ::rows to table: {table_name}")

            logger.info(f"Pushing chunk to table: {table_name}")
            total_time = (time.time() - start_time) / 60
            logger.info(f"Finished pushing data for {table_name} successfully.")
            logger.info(f"Total time taken: {total_time:.2f} minutes")

        except Exception as e:
            raise MyException(e)


class VendorSalesSummary:
    def __init__(self, connection):
        self.conn = connection

    def load_summary(self) -> DataFrame:
        query = """WITH FreightSummary AS (
            SELECT 
                VendorNumber, 
                SUM(Freight) AS FreightCost 
            FROM vendor_invoice 
            GROUP BY VendorNumber
        ), 
        
        PurchaseSummary AS (
            SELECT 
                p.VendorNumber,
                p.VendorName,
                p.Brand,
                p.Description,
                p.PurchasePrice,
                pp.Price AS ActualPrice,
                pp.Volume,
                SUM(p.Quantity) AS TotalPurchaseQuantity,
                SUM(p.Dollars) AS TotalPurchaseDollars
            FROM purchases p
            JOIN purchase_prices pp
                ON p.Brand = pp.Brand
            WHERE p.PurchasePrice > 0
            GROUP BY p.VendorNumber, p.VendorName, p.Brand, p.Description, p.PurchasePrice, pp.Price, pp.Volume
        ), 
        
        SalesSummary AS (
            SELECT 
                VendorNo,
                Brand,
                SUM(SalesQuantity) AS TotalSalesQuantity,
                SUM(SalesDollars) AS TotalSalesDollars,
                SUM(SalesPrice) AS TotalSalesPrice,
                SUM(ExciseTax) AS TotalExciseTax
            FROM sales
            GROUP BY VendorNo, Brand
        ) 
        
        SELECT 
            ps.VendorNumber,
            ps.VendorName,
            ps.Brand,
            ps.Description as BrandName,
            ps.PurchasePrice,
            ps.ActualPrice,
            ps.Volume,
            ps.TotalPurchaseQuantity,
            ps.TotalPurchaseDollars,
            ss.TotalSalesQuantity,
            ss.TotalSalesDollars,
            ss.TotalSalesPrice,
            ss.TotalExciseTax,
            fs.FreightCost
        FROM PurchaseSummary ps
        LEFT JOIN SalesSummary ss 
            ON ps.VendorNumber = ss.VendorNo 
            AND ps.Brand = ss.Brand
        LEFT JOIN FreightSummary fs 
            ON ps.VendorNumber = fs.VendorNumber
        ORDER BY ps.TotalPurchaseDollars DESC"""

        try:
            logger.info("VendorSalesSumary data fetchhing process started")
            start_time = time.time()
            df = pd.read_sql_query(query, self.conn)
            total_time = (time.time() - start_time) / 60
            logger.info(
                f"Vendor sales summary query executed in {total_time:.2f} minutes."
            )
            return df
        except Exception as e:
            raise MyException(e)


# client = MySqlClient(user="root", password="Suraj@2510", db="sales_data")
# engine = client.engine(with_db=True)

# raw_df = VendorSalesSummary(engine).load_summary()

# raw_df.head()
# if __name__ == "__main__":
#     client = MySqlClient(user="root", password="Suraj@2510", db="sales_data")
#     engine = client.engine(with_db=True)

#     raw_df = VendorSalesSummary(engine).load_summary()

#     ingestion = DataIngestion(
#         data_dir="src/components/data/data",
#         user="root",
#         password="Suraj@2510",
#         database="sales_data",
#     )
#     ingestion.ingest_all_files()
