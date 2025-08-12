from src.logger import config_logger
from src.exception import MyException

from sqlalchemy import text

logger = config_logger()


class SchemaManager:
    def __init__(self, engine, db):
        self.engine = engine
        self.db = db

    def Create_Database(self):
        try:
            if not self.db:
                raise ValueError(" Database not provided")

            logger.info(f"creating database if not exists{self.db}")

            engine = self.engine

            with engine.begin() as conn:
                conn.execute(text(f"CREATE DATABASE IF NOT EXISTS `{self.db}`;"))

                logger.info("data  base creatin sucessfuly done ")

        except Exception as e:
            raise MyException(e)


class VendorSalesSummaryTableCreator:
    def __init__(self, engine):
        """
        engine: SQLAlchemy Engine object
        """
        self.engine = engine

    def create_table(self, table_name: str):
        try:
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                VendorName VARCHAR(255),
                VendorNumber VARCHAR(255),
                Brand INT,
                BrandName VARCHAR(255),
                PurchasePrice DECIMAL(18,2),
                ActualPrice DECIMAL(18,2),
                Volume DECIMAL(18,2),
                TotalPurchaseQuantity INT,
                TotalPurchaseDollars DECIMAL(18,2),
                TotalSalesQuantity INT,
                TotalSalesDollars DECIMAL(18,2),
                TotalSalesPrice DECIMAL(18,2),
                TotalExciseTax DECIMAL(18,2),
                FreightCost DECIMAL(18,2),
                GrossProfit DECIMAL(18,2),
                ProfitMargin DECIMAL(10,2),
                StockTurnover DECIMAL(10,2),
                SalestoPurchaseRatio DECIMAL(10,2)
            );
            """
            logger.info(f"Creating table `{table_name}` if not exists.")
            with self.engine.connect() as conn:
                conn.execute(text(create_table_sql))

            logger.info(f"Table `{table_name}` created or already exists.")
        except Exception as e:
            raise MyException(e)
