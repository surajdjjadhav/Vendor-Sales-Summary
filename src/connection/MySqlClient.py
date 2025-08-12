from src.exception import MyException
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import pymysql
from src.components.schema_manager import SchemaManager
import warnings
import pandas as pd

from src.logger import config_logger

logger = config_logger()
warnings.filterwarnings("ignore", message="MYSQL_OPT_RECONNECT is deprecated")


class MySqlClient:
    """this calss used for making connection with mysql ,:
    creating database ,
    create a table
    """

    def __init__(
        self,
        user,
        password,
        db,
        host,
        port=3306,
    ):
        try:
            self.user = user
            self.password = quote_plus(password)
            self.db = db
            self.port = port
            self.host = host
            logger.info(" my sql credentials initilized")

        except Exception as e:
            raise MyException(e)

    def engine(self, with_db: bool = False):
        try:
            db_part = f"/{self.db}" if (with_db and self.db) else ""

            url = f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}{db_part}"

            logger.info("sql connection string initilized")

            engine = create_engine(url)

            logger.info("my sql connection done sucessfuly")

            return engine

        except Exception as e:
            raise MyException(e)


class DataPusher:
    def __init__(self, conn, database, table_name, if_exists):
        self.connection = conn
        self.database = database
        self.table_name = table_name
        self.if_exists = if_exists

    def push_data(self, data):
        conn = None
        try:
            logger.info(
                f"pushing data to table {self.table_name} in database {self.database}"
            )

            if data is None or data.empty:
                raise ValueError("your current dataframe is empty")

            conn = self.connection.connect()
            trans = conn.begin()

            data.to_sql(
                self.table_name,
                con=conn,
                if_exists=self.if_exists,
                index=False,
            )

            trans.commit()

            logger.info(
                f"data pushed to table {self.table_name} in database {self.database}"
            )

        except Exception as e:
            if conn is not None:
                trans.rollback()
            raise MyException(e)
        finally:
            if conn is not None:
                conn.close()
            if self.connection:
                self.connection.dispose()
                logger.info(f"connection to database {self.database} closed")

    def pull_data(self, table_name):
        try:
            query = f"SELECT * FROM {table_name}"

            logger.info(f"Executing query to pull data: {query}")

            with self.connection.connect() as conn:
                df = pd.read_sql_query(query, conn)

            logger.info(f"Data pulled successfully from {table_name}")

            return df

        except Exception as e:
            raise MyException(e)


if __name__ == "__main__":
    ingestion = MySqlClient(
        user="suraj",
        password="Suraj@2510",
        db="summary_data",
        host="localhost",
    )
    conn = ingestion.engine(with_db=False)
    schema = SchemaManager(conn, "summary_data")
    schema.Create_Database()

    ingestion = MySqlClient(
        user="suraj",
        password="Suraj@2510",
        db="summary_data",
        host="localhost",
    )
    conn_ = ingestion.engine(with_db=True)

    pusher = DataPusher(conn_, "summary_data", "data_analysis", if_exists="replace")

    data = pd.read_csv("clean_csv.csv")
    pusher.push_data(data=data)
