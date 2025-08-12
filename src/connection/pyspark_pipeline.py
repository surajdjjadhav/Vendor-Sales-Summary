import os
import pandas as pd
import pymysql
from pyspark.sql import SparkSession

# --- MySQL connection settings ---
user = "suraj"
password = "Suraj@2510"
host = "localhost"
database = "VendorSales_raw_data"

# --- Step 1: Create tables from CSV headers ---
conn = pymysql.connect(host=host, user=user, password=password)
query = "create database if not exists  VendorSales_raw_data"
cursor = conn.cursor()
cursor.execute(query)

conn = pymysql.connect(host=host, user=user, password=password, database=database)
cursor = conn.cursor()

data_folder = "../components/data/data"

for file in os.listdir(data_folder):
    if file.endswith(".csv"):
        file_path = os.path.join(data_folder, file)
        table_name = os.path.splitext(file)[0]  # CSV name without extension

        print(f"🛠 Creating table `{table_name}` in MySQL")

        # Read CSV headers
        df_sample = pd.read_csv(file_path, nrows=0)
        columns = df_sample.columns
        print(f"file_name{table_name} columns {columns}")

        # Build CREATE TABLE query dynamically (all columns as VARCHAR for simplicity)
        col_defs = ", ".join([f"`{col}` VARCHAR(255)" for col in columns])
        create_table_sql = f"CREATE TABLE IF NOT EXISTS `{table_name}` ({col_defs})"

        cursor.execute(create_table_sql)

conn.commit()
cursor.close()
conn.close()
print("✅ All tables created in MySQL")

import os
import time
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("MySQL_Connector_Test")
    .config("spark.jars", "/mnt/c/Users/suraj/Downloads/mysql-connector-j-8.0.33.jar")
    .config("spark.executor.cores", "4")
    .config("spark.driver.cores", "4")
    .getOrCreate()
)

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

        print(f"Row count in {file}: {df.count()}")
        df.show(5)

        df.write.format("jdbc").option(
            "url", f"jdbc:mysql://{host}:3306/{database}"
        ).option("dbtable", table_name).option("user", user).option(
            "password", password
        ).option("driver", "com.mysql.cj.jdbc.Driver").mode("append").save()

end_time = time.time()
print(f"Total time to push data: {(end_time - start_time) / 60:.2f} minutes")


class MySQLDataLoader:
    def __init__(self, data_folder, host, user, password, database, mysql_jar_path):
        self.data_folder = data_folder
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.mysql_jar_path = mysql_jar_path
        self.spark = self._create_spark_session()

    def _create_spark_session(self):
        return (
            SparkSession.builder.appName("MySQL_Connector_Test")
            .config("spark.jars", self.mysql_jar_path)
            .config("spark.executor.cores", "4")
            .config("spark.driver.cores", "4")
            .getOrCreate()
        )

    def create_database_and_tables(self):
        # Create DB if not exists and tables with columns as VARCHAR(255)
        conn = pymysql.connect(host=self.host, user=self.user, password=self.password)
        cursor = conn.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.database}")
        conn.commit()
        cursor.close()
        conn.close()

        conn = pymysql.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database,
        )
        cursor = conn.cursor()

        for file in os.listdir(self.data_folder):
            if file.endswith(".csv"):
                file_path = os.path.join(self.data_folder, file)
                table_name = os.path.splitext(file)[0]

                print(f"🛠 Creating table `{table_name}` in MySQL")

                df_sample = pd.read_csv(file_path, nrows=0)
                columns = df_sample.columns
                col_defs = ", ".join([f"`{col}` VARCHAR(255)" for col in columns])
                create_table_sql = (
                    f"CREATE TABLE IF NOT EXISTS `{table_name}` ({col_defs})"
                )

                cursor.execute(create_table_sql)

        conn.commit()
        cursor.close()
        conn.close()
        print("✅ All tables created in MySQL")

    def push_data_to_mysql(self):
        start_time = time.time()

        for file in os.listdir(self.data_folder):
            if file.endswith(".csv"):
                file_path = os.path.join(self.data_folder, file)
                table_name = os.path.splitext(file)[0]

                print(f"📥 Pushing data for `{table_name}`")

                df = self.spark.read.csv(file_path, header=True, inferSchema=True)

                df.write.format("jdbc").option(
                    "url", f"jdbc:mysql://{self.host}:3306/{self.database}"
                ).option("dbtable", table_name).option("user", self.user).option(
                    "password", self.password
                ).option("driver", "com.mysql.cj.jdbc.Driver").mode("append").save()

        end_time = time.time()
        print(
            f"⏳ Total time required to push data: {(end_time - start_time) / 60:.2f} minutes"
        )


if __name__ == "__main__":
    data_folder = "../components/data/data"
    host = "localhost"
    user = "root"
    password = "Suraj@2510"
    database = "VendorSales_raw_data"
    mysql_jar_path = "/mnt/c/Users/suraj/Downloads/mysql-connector-j-8.0.33.jar"

    loader = MySQLDataLoader(
        data_folder, host, user, password, database, mysql_jar_path
    )
    loader.create_database_and_tables()
    loader.push_data_to_mysql()
