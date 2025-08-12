import os
import pandas as pd
import pymysql
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id

# --- MySQL connection settings ---
user = "suraj"
password = "Suraj@2510"
host = "localhost"
database = "VendorSales_raw_data"

# --- Step 1: Create database if not exists ---
conn = pymysql.connect(host=host, user=user, password=password)
cursor = conn.cursor()
cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
conn.commit()
cursor.close()
conn.close()

# --- Step 2: Create tables with 'id' BIGINT PRIMARY KEY + CSV columns ---
conn = pymysql.connect(host=host, user=user, password=password, database=database)
cursor = conn.cursor()

data_folder = "../components/data/data"

for file in os.listdir(data_folder):
    if file.endswith(".csv"):
        file_path = os.path.join(data_folder, file)
        table_name = os.path.splitext(file)[0]

        print(f"🛠 Creating table `{table_name}` in MySQL")

        # Read CSV headers
        df_sample = pd.read_csv(file_path, nrows=0)
        columns = df_sample.columns

        # Build CREATE TABLE query with id BIGINT PRIMARY KEY
        col_defs = ", ".join([f"`{col}` VARCHAR(255)" for col in columns])
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS `{table_name}` (
            
            {col_defs} , spark_id BIGINT PRIMARY KEY
        )
        """
        cursor.execute(create_table_sql)

conn.commit()
cursor.close()
conn.close()
print("✅ All tables created in MySQL")

# --- Step 3: Initialize Spark session with MySQL driver ---

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, lit
from pyspark.sql.types import LongType
import os
import time

spark = (
    SparkSession.builder.appName("MySQL_Connector_Test")
    .config("spark.jars", "/mnt/c/Users/suraj/Downloads/mysql-connector-j-8.0.33.jar")
    .config("spark.executor.cores", "4")
    .config("spark.driver.cores", "4")
    .getOrCreate()
)

data_folder = "../components/data/data"
host = "localhost"
database = "VendorSales_raw_data"
user = "root"
password = "Suraj@2510"

start_time = time.time()

for file in os.listdir(data_folder):
    if file.endswith(".csv"):
        file_path = os.path.join(data_folder, file)
        table_name = os.path.splitext(file)[0]

        print(f"Reading file: {file_path}")
        df = spark.read.csv(file_path, header=True, inferSchema=True)

        # Add an incremental id column starting at 1
        window_spec = Window.orderBy(
            lit("A")
        )  # Order by a constant (all in one window)
        df = df.withColumn("spark_id", row_number().over(window_spec).cast(LongType()))

        total_rows = df.count()
        print(f"Total rows in {file}: {total_rows}")

        rows_per_partition = 200_000
        partitions = max(4, total_rows // rows_per_partition)

        # Repartition on spark_id for parallel write
        df = df.repartition(partitions, "spark_id").cache()
        df.count()  # materialize cache

        df.write.format("jdbc").option(
            "url", f"jdbc:mysql://{host}:3306/{database}"
        ).option("dbtable", table_name).option("user", user).option(
            "password", password
        ).option("driver", "com.mysql.cj.jdbc.Driver").option(
            "numPartitions", partitions
        ).option("partitionColumn", "spark_id").option("lowerBound", 1).option(
            "upperBound", total_rows
        ).mode("append").save()

end_time = time.time()
print(f"Total time to push data: {(end_time - start_time) / 60:.2f} minutes")
