import subprocess
from utils.logger import setup_logger

class HiveManager:
    def __init__(self):
        self.logger = setup_logger("HiveManager")
        self.logger.info("HiveManager initialized - SIMPLE")

    def execute_hive_command(self, hive_sql):
        try:
            self.logger.info(f"Executing Hive: {hive_sql}")
            result = subprocess.run(
                ['hive', '-e', hive_sql],
                capture_output=True,
                text=True,
                timeout=600
            )
            if result.returncode == 0:
                self.logger.info("Hive command OK")
                return result.stdout
            else:
                self.logger.error(f"Hive failed: {result.stderr}")
                raise Exception(f"Hive failed: {result.stderr}")
        except Exception as e:
            self.logger.error(f"Hive error: {e}")
            raise

    def create_database(self, db_name):
        self.logger.info(f"Creating DB: {db_name}")
        self.execute_hive_command(f"CREATE DATABASE IF NOT EXISTS {db_name};")

    def create_table_from_parquet(self, db_name, table_name, parquet_path, schema_sql):
        self.logger.info(f"Creating table {table_name}")
        self.execute_hive_command(f"USE {db_name}; DROP TABLE IF EXISTS {table_name};")
        create_sql = f"""
            USE {db_name};
            CREATE EXTERNAL TABLE {table_name} (
                {schema_sql}
            )
            STORED AS PARQUET
            LOCATION '{parquet_path}';
        """
        self.execute_hive_command(create_sql)
        self.logger.info(f"Table {table_name} created")


    def show_tables(self, db_name):
        return self.execute_hive_command(f"USE {db_name}; SHOW TABLES;")
