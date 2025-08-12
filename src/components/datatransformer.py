import pandas as pd
import numpy as np
from src.logger import config_logger
from src.exception import MyException

logger = config_logger()


class VendorDataTransformer:
    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()

    def transform(self) -> pd.DataFrame:
        try:
            logger.info("Starting data transformation")

            # Gross Profit = Sales - Purchase
            self.df["GrossProfit"] = (
                self.df["TotalSalesDollars"] - self.df["TotalPurchaseDollars"]
            )

            # Profit Margin (%) - avoid division by zero
            self.df["ProfitMargin"] = np.where(
                self.df["TotalSalesDollars"] != 0,
                (self.df["GrossProfit"] / self.df["TotalSalesDollars"]) * 100,
                0,
            )

            # Stock Turnover - avoid division by zero
            self.df["StockTurnover"] = np.where(
                self.df["TotalPurchaseQuantity"] != 0,
                self.df["TotalSalesQuantity"] / self.df["TotalPurchaseQuantity"],
                0,
            )

            # Sales-to-Purchase Ratio - avoid division by zero
            self.df["SalestoPurchaseRatio"] = np.where(
                self.df["TotalPurchaseDollars"] != 0,
                self.df["TotalSalesDollars"] / self.df["TotalPurchaseDollars"],
                0,
            )

            # Final cleanup: replace inf/-inf & NaN with 0
            self.df.replace([np.inf, -np.inf], 0, inplace=True)
            self.df.fillna(0, inplace=True)

            # Round all numeric columns to 2 decimals
            num_cols = self.df.select_dtypes(include=[np.number]).columns
            self.df[num_cols] = self.df[num_cols].round(2)

            logger.info("Data transformation complete")
            logger.info(f"ProfitMargin at row 354: {self.df.loc[353, 'ProfitMargin']}")
            logger.info(f"ProfitMargin at row 290: {self.df.loc[290, 'ProfitMargin']}")

            return self.df

        except Exception as e:
            raise MyException(e)
