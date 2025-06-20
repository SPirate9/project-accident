from pyspark.sql import functions as F
from pyspark.sql.types import *
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
        self.logger.info("DataPreprocessor initialized")
        self.hive_manager.create_database(Config.HIVE_DB)

    def read_latest_bronze_data(self):
        latest_date = Config.SIMULATION_DATES[-1]
        bronze_path = Config.get_bronze_path(latest_date)
        self.logger.info(f"Reading Bronze: {bronze_path}")
        df = self.spark.read.parquet(bronze_path)
        count = df.count()
        self.logger.info(f"Bronze loaded: {count} rows")
        return df

    def intelligent_cleaning(self, df):
        """Nettoyage basé sur l'analyse des nulls"""
        self.logger.info("Starting cleaning")
        initial_count = df.count()
        
        # 1. Filtrer les lignes avec nulls CRITIQUES (colonnes essentielles)
        self.logger.info("Filtering critical nulls")
        df_clean = df.filter(
            F.col("ID").isNotNull() &
            F.col("Severity").isNotNull() &
            F.col("State").isNotNull() &
            F.col("Start_Lat").isNotNull() &
            F.col("Start_Lng").isNotNull() &
            F.col("Start_Time").isNotNull()
        )
        
        after_critical = df_clean.count()
        self.logger.info(f"After filtering critical nulls: {after_critical} rows (removed: {initial_count - after_critical})")
        
        # 2. Nettoyer les noms de colonnes AVANT fillna
        self.logger.info("Cleaning column names first...")
        for field in df_clean.schema.fields:
            old_name = field.name
            new_name = self.clean_column_name(old_name)
            if old_name != new_name:
                df_clean = df_clean.withColumnRenamed(old_name, new_name)
        
        # 3. Géo-spatiales : End_Lat/End_Lng (44% nulls) -> utiliser Start_Lat/Start_Lng
        df_clean = df_clean.withColumn("end_lat", 
            F.when(F.col("end_lat").isNull(), F.col("start_lat")).otherwise(F.col("end_lat")))
        df_clean = df_clean.withColumn("end_lng", 
            F.when(F.col("end_lng").isNull(), F.col("start_lng")).otherwise(F.col("end_lng")))
        
        # 4. Remplacer les valeurs manquantes par des valeurs par défaut intelligentes (avec noms nettoyés)
        self.logger.info("Replacing missing values with intelligent defaults")
        
        # Météo : valeurs par défaut basées sur des moyennes réalistes
        df_clean = df_clean.fillna({
            'temperature_f': 70.0,  # Température moyenne US
            'humidity': 60.0,     # Humidité moyenne
            'pressure_in': 29.92,   # Pression atmosphérique standard
            'visibility_mi': 10.0,  # Visibilité normale
            'wind_speed_mph': 5.0,  # Vent léger
            'weather_condition': 'Clear',
            'wind_direction': 'CALM'
        })
        
        # Textuelles : valeurs par défaut informatives
        df_clean = df_clean.fillna({
            'description': 'No description available',
            'street': 'Unknown Street',
            'zipcode': '00000',
            'timezone': 'US/Eastern',
            'airport_code': 'UNKNOWN'
        })
        
        # Temporelles : valeurs par défaut
        df_clean = df_clean.fillna({
            'sunrise_sunset': 'Day',
            'civil_twilight': 'Day',
            'nautical_twilight': 'Day',
            'astronomical_twilight': 'Day'
        })
        
        # 5. Supprimer les doublons (basé sur ID qui est unique)
        self.logger.info("Removing duplicates")
        before_dedup = df_clean.count()
        df_clean = df_clean.dropDuplicates(["id"])
        after_dedup = df_clean.count()
        self.logger.info(f"Duplicates removed: {before_dedup - after_dedup}")
        
        # 6. Normaliser les données textuelles
        self.logger.info("Normalizing text data")
        df_clean = df_clean.withColumn("state", F.upper(F.trim(F.col("state"))))
        df_clean = df_clean.withColumn("weather_condition", F.upper(F.trim(F.col("weather_condition"))))
        df_clean = df_clean.withColumn("wind_direction", F.upper(F.trim(F.col("wind_direction"))))
        
        # 7. Valider les valeurs de Severity (doit être 1-4)
        self.logger.info("Validating Severity values")
        df_clean = df_clean.filter(F.col("severity").between(1, 4))
        
        # 8. Valider les coordonnées géographiques (USA bounds approximatifs)
        self.logger.info("Validating geographic coordinates")
        df_clean = df_clean.filter(
            F.col("start_lat").between(24.0, 50.0) &  # Latitude USA
            F.col("start_lng").between(-130.0, -65.0)  # Longitude USA
        )
        
        final_count = df_clean.count()
        removed_total = initial_count - final_count
        self.logger.info(f"Cleaning completed:")
        self.logger.info(f"  - Initial: {initial_count:,} rows")
        self.logger.info(f"  - Final: {final_count:,} rows")
        self.logger.info(f"  - Removed: {removed_total:,} rows ({removed_total/initial_count*100:.2f}%)")
        
        return df_clean
    
    def cast_data_types(self, df):
        """Cast des types pour optimiser le stockage"""
        self.logger.info("Casting data types for optimization")
        
        # Cast explicites pour Hive compatibility
        df = df.withColumn("severity", F.col("severity").cast(IntegerType()))
        df = df.withColumn("start_lat", F.col("start_lat").cast(DoubleType()))
        df = df.withColumn("start_lng", F.col("start_lng").cast(DoubleType()))
        df = df.withColumn("end_lat", F.col("end_lat").cast(DoubleType()))
        df = df.withColumn("end_lng", F.col("end_lng").cast(DoubleType()))
        df = df.withColumn("distance_mi", F.col("distance_mi").cast(DoubleType()))
        df = df.withColumn("temperature_f", F.col("temperature_f").cast(DoubleType()))
        df = df.withColumn("humidity", F.col("humidity").cast(DoubleType()))
        df = df.withColumn("pressure_in", F.col("pressure_in").cast(DoubleType()))
        df = df.withColumn("visibility_mi", F.col("visibility_mi").cast(DoubleType()))
        df = df.withColumn("wind_speed_mph", F.col("wind_speed_mph").cast(DoubleType()))
        
        # Cast des booléens
        boolean_cols = ['amenity', 'bump', 'crossing', 'give_way', 'junction', 
                       'no_exit', 'railway', 'roundabout', 'station', 'stop', 
                       'traffic_calming', 'traffic_signal', 'turning_loop']
        
        for col in boolean_cols:
            if col in df.columns:
                df = df.withColumn(col, F.col(col).cast(BooleanType()))
        
        # Cast des timestamps
        df = df.withColumn("start_time", F.col("start_time").cast(TimestampType()))
        df = df.withColumn("end_time", F.col("end_time").cast(TimestampType()))
        if "weather_timestamp" in df.columns:
            df = df.withColumn("weather_timestamp", F.col("weather_timestamp").cast(TimestampType()))
        
        self.logger.info("Data types casting completed")
        return df
    
    def clean_column_name(self, name):
        """Nettoie les noms de colonnes pour Hive"""
        cleaned = re.sub(r'[^0-9a-zA-Z_]', '_', name)
        cleaned = re.sub(r'_+', '_', cleaned)
        cleaned = cleaned.rstrip('_')
        return cleaned.lower()
    
    def save_to_hive_simple(self, df):
        table_name = "accidents_silver"
        silver_path = f"{Config.SILVER_PATH}/simple"
        self.logger.info(f"Saving to Hive: {table_name}")
        
        # Cast types avant sauvegarde
        df_typed = self.cast_data_types(df)
        
        # Sauvegarder en Parquet
        df_typed.write.mode("overwrite").parquet(silver_path)

        # Extract schema for Hive
        schema_sql = ",\n                ".join([
            f"{field.name} {self.convert_spark_type_to_hive(field.dataType)}" 
            for field in df_typed.schema.fields
        ])
        
        self.hive_manager.create_table_from_parquet(
            db_name=Config.HIVE_DB,
            table_name=table_name,
            parquet_path=silver_path,
            schema_sql=schema_sql
        )
       
        final_count = df_typed.count()
        self.logger.info(f"Hive table {table_name} created successfully with {final_count:,} rows")
        return final_count
    
    def convert_spark_type_to_hive(self, spark_type):
        """Convertit les types Spark vers Hive"""
        type_str = spark_type.simpleString().lower()
        
        type_mapping = {
            'bigint': 'bigint',
            'int': 'int', 
            'integer': 'int',
            'double': 'double',
            'float': 'float',
            'string': 'string',
            'boolean': 'boolean',
            'timestamp': 'timestamp',
            'date': 'date'
        }
        
        return type_mapping.get(type_str, 'string')

    def run_preprocessing_pipeline(self):
        self.logger.info("Starting Silver pipeline")
        try:
            # 1. Lire Bronze
            df_bronze = self.read_latest_bronze_data()
            
            # 2. Nettoyage 
            df_clean = self.intelligent_cleaning(df_bronze)
            
            # 3. Sauvegarder en Hive
            final_count = self.save_to_hive_simple(df_clean)
            
            # 4. Afficher tables
            tables = self.hive_manager.show_tables(Config.HIVE_DB)
            self.logger.info(f"Hive tables:\n{tables}")
            
            self.logger.info(f"Silver pipeline completed: {final_count:,} rows")
            
        except Exception as e:
            self.logger.error(f"Pipeline error: {e}")
            raise

    def close(self):
        if self.spark:
            self.spark.stop()
            self.logger.info("Closed")