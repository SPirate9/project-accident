from pyspark.sql import functions as F
from config.config import Config
from utils.logger import setup_logger
from utils.spark_utils import create_spark_session
from utils.hive_manager import HiveManager
import re

class DataPreprocessor:
    def __init__(self):
        self.logger = setup_logger("DataPreprocessor")
        self.spark = create_spark_session("DataPreprocessor")
        self.hive_manager = HiveManager()
        self.logger.info("DataPreprocessor SIMPLE initialized")
        self.hive_manager.create_database(Config.HIVE_DB)

    def read_latest_bronze_data(self):
        latest_date = Config.SIMULATION_DATES[-1]
        bronze_path = Config.get_bronze_path(latest_date)
        self.logger.info(f"Reading Bronze: {bronze_path}")
        df = self.spark.read.parquet(bronze_path)
        count = df.count()
        self.logger.info(f"Bronze loaded: {count} rows")
        return df

    def basic_cleaning(self, df):
        self.logger.info("Basic cleaning")
        df_clean = df.filter(
            F.col("ID").isNotNull() &
            F.col("State").isNotNull()
        )
        count = df_clean.count()
        self.logger.info(f"After cleaning: {count} rows")
        return df_clean
    
    def clean_column_name(self, name):
        self.logger.info(f"Cleaning column name: {name}")
        cleaned = re.sub(r'[^0-9a-zA-Z_]', '_', name)
        cleaned = re.sub(r'_+', '_', cleaned)
        cleaned = cleaned.rstrip('_')
        return cleaned
    
    def save_to_hive_simple(self, df):
        table_name = "accidents_silver"
        silver_path = f"{Config.SILVER_PATH}/simple"
        self.logger.info(f"Saving to Hive: {table_name}")
        df.write.mode("overwrite").parquet(silver_path)

        # Extract schema for Hive
        schema_sql = ",\n                ".join([
            f"{self.clean_column_name(field.name)} {field.dataType.simpleString()}" 
            for field in df.schema.fields
    ])
        self.hive_manager.create_table_from_parquet(
            db_name=Config.HIVE_DB,
            table_name=table_name,
            parquet_path=silver_path,
            schema_sql=schema_sql
        )
       
        self.logger.info(f"Hive table {table_name} created successfully")
    

    def run_preprocessing_pipeline(self):
        self.logger.info("Starting SIMPLE pipeline")
        try:
            df_bronze = self.read_latest_bronze_data()
            df_clean = self.basic_cleaning(df_bronze)
            final_count = self.save_to_hive_simple(df_clean)
            tables = self.hive_manager.show_tables(Config.HIVE_DB)
            self.logger.info(f"Hive tables:\n{tables}")
            self.logger.info(f"SIMPLE pipeline completed: {final_count} rows")
        except Exception as e:
            self.logger.error(f"Pipeline error: {e}")
            raise

    def close(self):
        if self.spark:
            self.spark.stop()
            self.logger.info("Closed")
