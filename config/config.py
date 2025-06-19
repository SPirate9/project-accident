import os

class Config:
    HDFS_BASE = "hdfs://localhost:9000"
    BRONZE_PATH = f"{HDFS_BASE}/user/saad/bronze"
    SILVER_PATH = f"{HDFS_BASE}/user/saad/silver"
    GOLD_PATH = f"{HDFS_BASE}/user/saad/gold"
    
    # AJOUTER: Configuration Hive
    HIVE_DB = "accident_analysis_db"
    HIVE_WAREHOUSE = f"{HDFS_BASE}/user/hive/warehouse"
    
    SOURCE_FILE = f"file://{os.getcwd()}/data/US_Accidents_March23.csv"
    APP_NAME = "AccidentAnalysis"
    
    SIMULATION_DATES = ["2025-01-01", "2025-01-02", "2025-01-03"]
    
    @staticmethod
    def get_bronze_path(date_str):
        year, month, day = date_str.split('-')
        return f"{Config.BRONZE_PATH}/{year}/{month}/{day}"
    
    # AJOUTER: Méthode pour Silver
    @staticmethod
    def get_silver_path(date_str):
        return f"{Config.SILVER_PATH}/{date_str.replace('-', '_')}"