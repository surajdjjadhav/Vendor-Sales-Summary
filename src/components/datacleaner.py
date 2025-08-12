import pandas as pd
from src.logger import config_logger
from src.exception import MyException

logger = config_logger()


class VendorDataCleaner:
    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()

    def clean(self) -> pd.DataFrame:
        try:
            logger.info("Starting data cleaning")

            self.df.replace([float("inf"), float("-inf")], pd.NA, inplace=True)

            self.df.fillna(0, inplace=True)

            if self.df["Volume"].dtype == "object":
                self.df["Volume"] = self.df["Volume"].astype("float64")

            self.df["VendorName"] = self.df["VendorName"].str.strip()

            logger.info("Data cleaning complete")
            return self.df

        except Exception as e:
            raise MyException(e)
