import pandas as pd
from src.components.data_injetion import VendorSalesSummary
from src.components.data_injetion import DataPusher
from src.components.datatransformer import VendorDataTransformer
from src.components.datacleaner import VendorDataCleaner
from src.connection.MySqlClient import MySqlClient
from src.components.schema_manager import VendorSalesSummaryTableCreator


client = MySqlClient("suraj", "Suraj@2510", "sales_data", host="localhost")
con = client.engine(with_db=True)

table = pd.read_sql_query("select name from sqlite_master where type = 'table'", con)
table

for tab in table["name"]:
    print("-" * 20, tab, "-" * 50)
    print(
        f"count of records {tab} : ",
        pd.read_sql_query(f"select count(*) as count from {tab}", con).values[0],
    )


for tab in table["name"]:
    print("-" * 20, tab, "-" * 50)
    df = pd.read_sql_query(f"select * from {tab} limit 5", con)
    display(df)

# ------------------------------------------ Exploring data tables for analysis-------------------------------------
sales = pd.read_sql_query("select * from sales where VendorNo= 4466", con)

purchases = pd.read_sql_query("select * from purchases where VendorNumber = 4466 ", con)
purchase_prices = pd.read_sql_query(
    "select * from purchase_prices where VendorNumber = 4466 ", con
)
vendor_invoice = pd.read_sql_query(
    "select * from vendor_invoice where VendorNumber = 4466 ", con
)

purchases.groupby(["Brand", "PurchasePrice"])[["Quantity", "Dollars"]].sum()
purchase_prices
vendor_invoice["PONumber"].nunique()  # this is unique id in this table
vendor_invoice.shape

sales.groupby(["Brand"])[["SalesDollars", "SalesQuantity", "SalesPrice"]].sum()
sales.columns


exploratory_insights = """
    1. purchase table contain actual purchase data including date fo purchase  prooduct(brand) purchase by vendor the amount paid in dollars  and the quantity purchased 
    2 . purchasePrie column for  ud derived from purchase price table  ,which provide product wise actual purchase price the combination of vendor and brand is unique in the table
    3. the vendor inovice table aggrigate data form purchase table , summirizing quantity and dollar amount ,  along with the additional column for frieght .
        This table maintain    uniqueness based on vendor and po number 
    4 . The sales thable capturre actual sales transaction   , dealing the brand purchased by vendors the quantitty sold  , the selling price and revenue earned
    
    -------------------------------------------------------------------------------------------
    # as data needed for analysis distributed in diffrent tables  , we need to create summary table  containing 
    1. purchase transaction made by vendor 
    2. sales Transaction data 
    3.freight cost for each vendor 
    4. actual product price per vendor 
    """

print(exploratory_insights)


# --------------------------------------------- query performace ooptimization ------------------------------------------------------
performance = """

1. The query involves heavy joins and aggrigations  on large data set like sales and purchase 
2. storing pre aggrigated results to avoidrepetative expensive computation 
3. helps in analyzing sales , purchase , and pricing for diffrent vendor and brands 
4.  future Benefits of storing  this data  for faster dashboarding  and reporting 
5 . insted of running expensinve queary each time  dashboard can fetch data from  vendor sales summary 
"""
print(performance)


# -------------------------------------------  data cleaning ------------------------------------------------------------------------
client = MySqlClient("suraj", "Suraj@2510", "sales_data", host="localhost")
engine = client.engine(with_db=True)
vs = VendorSalesSummary(engine)

vendor_sales_summary = vs.load_summary()
vendor_sales_summary.shape
vendor_sales_summary.isnull().sum()
# null value present in columns TotalSalesQuantuty , totalsalesdollar , totalsalesprice , totalexcisetax
# those values are null calu vendor purchase those products but not sell this products

vendor_sales_summary.dtypes  # -- datatypes of Volumen column is object but this column have numerical value

vendor_sales_summary["Volume"].str.replace(r"[0-9]", "", regex=True).unique()
# all numeric value presentthis column

vendor_sales_summary["Volume"] = vendor_sales_summary["Volume"].astype("float64")

vendor_sales_summary["VendorName"].unique()  # -- rewure trim white speaces
vendor_sales_summary["VendorName"] = vendor_sales_summary["VendorName"].str.strip()

vendor_sales_summary["Description"].unique()

vendor_sales_summary.fillna(0, inplace=True)
vendor_sales_summary.isnull().sum()


# -- ----------------------- Feature Engineering ----------------------

# ----- Gross Profit  = total sales - total purchase

vendor_sales_summary["GrossProfit"] = (
    vendor_sales_summary["TotalSalesDollars"]
    - vendor_sales_summary["TotalPurchaseDollars"]
)

loss_occuring = vendor_sales_summary[vendor_sales_summary["GrossProfit"] < 0][
    "GrossProfit"
].index
vendor_sales_summary.loc[loss_occuring]

# ------- (Profit margin = Gross Proft / total Sales  ) * 100

vendor_sales_summary["ProfitMargin"] = (
    vendor_sales_summary["GrossProfit"] / vendor_sales_summary["TotalSalesDollars"]
) * 100
vendor_sales_summary["ProfitMargin"]

vendor_sales_summary["StockTurnover"] = (
    vendor_sales_summary["TotalSalesQuantity"]
    / vendor_sales_summary["TotalPurchaseQuantity"]
)
vendor_sales_summary["StockTurnover"]

vendor_sales_summary["SalestoPurchaseRatio"] = (
    vendor_sales_summary["TotalSalesDollars"]
    / vendor_sales_summary["TotalPurchaseDollars"]
)
vendor_sales_summary["SalestoPurchaseRatio"]
vendor_sales_summary.columns


# -------------------------- create table for pushing transformed data -------------------------------------------
Table = VendorSalesSummaryTableCreator(con)
Table.create_table("VendorSalesSummay")
vendor_summary_sales = pd.read_sql_query("select * from VendorSalesSummay", con)


# ---------------------------------- clean data -------------------------
clean = VendorDataCleaner(df=vendor_sales_summary)
clean_df = clean.clean()


# ----------------------------------- transform data-------------------------------------------------------
transformer = VendorDataTransformer(df=clean_df)
finnal_df = transformer.transform()


# ------------------------------------------- push transformed data ----------------------------------
pusher = DataPusher(
    "database/vendor_performance.db",
    table_name="VendorSalesSummay",
    if_exists="replace",
)
pusher.push_data(finnal_df)


# -----------------------------------------------save transformed data as csv file ------------------------------------------------------
vendor_summary_sales.head()
vendor_summary_sales.to_csv("data.csv", index=False)
