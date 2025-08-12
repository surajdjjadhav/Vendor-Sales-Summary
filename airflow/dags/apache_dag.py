from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from datetime import timedelta


from src.logger import config_logger
from src.exception import MyException
from src.components.data_injetion import VendorSalesSummary, DataIngestion
from src.components.datatransformer import VendorDataTransformer
from src.components.datacleaner import VendorDataCleaner
from src.connection.MySqlClient import MySqlClient, DataPusher
from src.components.schema_manager import SchemaManager
from src.components.schema_manager import VendorSalesSummaryTableCreator

# Replace with your actual MySQL credentials/constants


logger = config_logger()

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": days_ago(1),
}


@dag(
    dag_id="vendor_performance_pipeline",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",
    catchup=False,
    tags=["vendor", "etl", "sqlite", "mysql"],
    dagrun_timeout=timedelta(hours=1),
)
def vendor_pipeline():
    @task(execution_timeout=timedelta(minutes=5))
    def create_mysql_database(database, user, password):
        try:
            client = MySqlClient(
                user=user, password=password, db=database, host="mysql"
            )
            engine = client.engine(with_db=False)
            schema = SchemaManager(engine, database)
            schema.Create_Database()
        except Exception as e:
            raise MyException(e)

    # -- do this step
    @task(execution_timeout=timedelta(minutes=20))
    def inject_data_to_sql(
        data_dir,
        user,
        password,
        database,
    ):
        try:
            logger.info("Ingesting raw data into SQLite...")
            conn = MySqlClient(user, password, database, host="mysql").engine(
                with_db=True
            )
            ingestion = DataIngestion(data_dir, conn, database)
            ingestion.ingest_all_files()
            logger.info("Data ingestion complete.")
        except Exception as e:
            raise MyException(e)

    @task(execution_timeout=timedelta(minutes=7))
    def clean_data(user, password, db_name):
        try:
            logger.info("Cleaning data from SQLite...")

            client = MySqlClient(user=user, password=password, db=db_name, host="mysql")
            engine = client.engine(with_db=True)

            raw_df = VendorSalesSummary(engine).load_summary()
            logger.info(f" sahpe beffor cleaing of vendorsalessumary :: {raw_df.shape}")
            cleaned_df = VendorDataCleaner(df=raw_df).clean()
            logger.info(
                f" sahpe after cleaing of vendorsalessumary :: {cleaned_df.shape}"
            )

            logger.info("Data cleaning complete.")

            pusher = DataPusher(
                engine,
                db_name,
                table_name="clean_data",
                if_exists="replace",
            )
            pusher.push_data(cleaned_df)
        except Exception as e:
            raise MyException(e)

    @task(execution_timeout=timedelta(minutes=10))
    def create_table(table_name, db_name, user, password):
        try:
            client = MySqlClient(user=user, password=password, db=db_name, host="mysql")
            engine = client.engine(with_db=True)

            table_creator = VendorSalesSummaryTableCreator(engine)
            table_creator.create_table(table_name=table_name)
        except Exception as e:
            raise MyException(e)

    @task(execution_timeout=timedelta(minutes=7))
    def transform_data(user, password, db_name):
        client = MySqlClient(user=user, password=password, db=db_name, host="mysql")
        engine = client.engine(with_db=True)
        pusher = DataPusher(
            engine,
            db_name,
            table_name="clean_data",
            if_exists="replace",
        )
        df = pusher.pull_data("clean_data")
        try:
            logger.info("Transforming data...")
            transformed_df = VendorDataTransformer(df=df).transform()
            logger.info("Data transformation complete.")
            pusher = DataPusher(
                engine,
                db_name,
                table_name="vendor_sales_summary",
                if_exists="append",
            )
            pusher.push_data(transformed_df)

            return
        except Exception as e:
            raise MyException(e)

    # Parameters
    data_dir_path = "src/components/data/data"

    from src.constrains import user, password, db_name, table_name

    # DAG flow
    db_creation = create_mysql_database(user=user, password=password, database=db_name)
    ingestion = inject_data_to_sql(
        data_dir=data_dir_path, user=user, password=password, database=db_name
    )
    cleaned = clean_data(user=user, password=password, db_name=db_name)
    table_created = create_table(
        table_name=table_name, db_name=db_name, user=user, password=password
    )

    transformed = transform_data(user=user, password=password, db_name=db_name)

    db_creation >> ingestion >> cleaned >> table_created >> transformed


dag_instance = vendor_pipeline()
