from pyspark.sql import functions as F
from config.config import Config
from utils.logger import setup_logger
from utils.spark_utils import create_spark_session

class DataExporter:
    def __init__(self):
        self.logger = setup_logger("DataExporter")
        self.spark = create_spark_session("DataExporter")
        self.logger.info("DataExporter initialized - HIVE → PARQUET → POSTGRES")
        
        # Config PostgreSQL
        self.postgres_config = {
            "url": "jdbc:postgresql://localhost:5432/accident_viz",
            "user": "saad",
            "password": "saad",
            "driver": "org.postgresql.Driver"
        }
        
        # Chemin des datamarts Parquet sur HDFS
        self.gold_path = Config.GOLD_PATH
        self.logger.info(f"Using GOLD path: {self.gold_path}")
    
    def read_silver_data(self):
        """Lit Silver depuis Hive"""
        self.logger.info("Reading Silver data from Hive")
        
        df = self.spark.sql(f"SELECT * FROM {Config.HIVE_DB}.accidents_silver")
        count = df.count()
        self.logger.info(f"Silver loaded: {count:,} rows")
        
        return df
    
    def create_state_stats(self, df):
        """Stats par état"""
        self.logger.info("Creating state stats")
        
        return df.groupBy("state") \
            .agg(
                F.count("*").alias("total_accidents"),
                F.avg("severity").alias("avg_severity"),
                F.countDistinct("city").alias("cities_count")
            ) \
            .orderBy(F.desc("total_accidents"))
    
    def create_time_stats(self, df):
        """Stats temporelles"""
        self.logger.info("Creating time stats")
        
        return df.withColumn("hour", F.hour("start_time")) \
                 .withColumn("day_of_week", F.dayofweek("start_time")) \
                 .groupBy("hour", "day_of_week") \
                 .agg(
                     F.count("*").alias("total_accidents"),
                     F.avg("severity").alias("avg_severity")
                 )
    
    def create_weather_stats(self, df):
        """Stats météo"""
        self.logger.info("Creating weather stats")
        
        return df.filter(F.col("weather_condition").isNotNull()) \
                 .groupBy("weather_condition") \
                 .agg(
                     F.count("*").alias("total_accidents"),
                     F.avg("severity").alias("avg_severity"),
                     F.avg("temperature_f").alias("avg_temperature")
                 ) \
                 .orderBy(F.desc("total_accidents"))
    
    def create_city_hotspots(self, df):
        """Top villes"""
        self.logger.info("Creating city hotspots")
        
        return df.groupBy("state", "city") \
                 .agg(
                     F.count("*").alias("total_accidents"),
                     F.avg("severity").alias("avg_severity"),
                     F.avg("start_lat").alias("latitude"),
                     F.avg("start_lng").alias("longitude")
                 ) \
                 .filter(F.col("total_accidents") >= 50) \
                 .orderBy(F.desc("total_accidents")) \
                 .limit(200)
    
    
    def save_to_gold_parquet(self, df, table_name):
        """Sauvegarde datamart en Parquet dans GOLD"""
        parquet_path = Config.get_gold_path(table_name)
        self.logger.info(f"Saving {table_name} to GOLD Parquet: {parquet_path}")
        # Optimiser et sauvegarder
        df.coalesce(2) \
          .write \
          .mode("overwrite") \
          .parquet(parquet_path)
        
        count = df.count()
        self.logger.info(f"{table_name}: {count:,} rows saved to GOLD")
        return parquet_path

    def read_from_gold_parquet(self, table_name):
        """Lit datamart depuis GOLD Parquet"""
        parquet_path = Config.get_gold_path(table_name)
        self.logger.info(f"Reading {table_name} from GOLD: {parquet_path}")
        
        df = self.spark.read.parquet(parquet_path)
        count = df.count()
        self.logger.info(f"{table_name}: {count:,} rows loaded from GOLD")
        return df
    
    def export_to_postgres(self, df, table_name):
        """Export vers PostgreSQL"""
        self.logger.info(f"Exporting {table_name} to PostgreSQL")
        
        try:
            df.write \
                .format("jdbc") \
                .option("url", self.postgres_config["url"]) \
                .option("dbtable", table_name) \
                .option("user", self.postgres_config["user"]) \
                .option("password", self.postgres_config["password"]) \
                .option("driver", self.postgres_config["driver"]) \
                .mode("overwrite") \
                .save()
            
            count = df.count()
            self.logger.info(f"{table_name}: {count:,} rows exported to PostgreSQL")
            
        except Exception as e:
            self.logger.error(f"Error exporting {table_name} to PostgreSQL: {e}")
            # Fallback CSV
            self.export_to_csv(df, table_name)
    
    def export_to_csv(self, df, table_name):
        """Fallback CSV"""
        self.logger.info(f"Fallback: Exporting {table_name} to CSV")
        
        df.coalesce(1) \
            .write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(f"./exports/{table_name}")
        
        count = df.count()
        self.logger.info(f"{table_name}: {count:,} rows to CSV")
    
    def run_aggregations(self):
        """ÉTAPE 1: Créer agrégations et sauver en Parquet"""
        self.logger.info("=== ÉTAPE 1: AGRÉGATIONS HIVE → PARQUET ===")
        
        try:
            # 1. Lire Silver depuis Hive
            df_silver = self.read_silver_data()
            
            # 2. Créer les datamarts
            self.logger.info("Creating datamarts...")
            
            df_state = self.create_state_stats(df_silver)
            df_time = self.create_time_stats(df_silver)
            df_weather = self.create_weather_stats(df_silver)
            df_cities = self.create_city_hotspots(df_silver)
            
            # 3. Sauvegarder en Parquet sur HDFS
            self.logger.info("Saving to GOLD layer...")
            
            self.save_to_gold_parquet(df_state, "state_stats")
            self.save_to_gold_parquet(df_time, "time_patterns")
            self.save_to_gold_parquet(df_weather, "weather_analysis")
            self.save_to_gold_parquet(df_cities, "city_hotspots")
            
            self.logger.info("Agrégations completed - Datamarts saved to GOLD layer")
            
        except Exception as e:
            self.logger.error(f"Aggregations failed: {e}")
            raise
    
    def run_postgres_export(self):
        """ÉTAPE 2: Lire Parquet et exporter vers PostgreSQL"""
        self.logger.info("=== ÉTAPE 2: GOLD PARQUET → POSTGRESQL ===")
        
        try:
            datamarts = [
                "state_stats", "time_patterns", "weather_analysis", "city_hotspots",
                "ml_severity_predictions", "ml_risk_zones", "ml_hotspots"
            ]
            
            for table_name in datamarts:
                # 1. Lire depuis Parquet HDFS
                df = self.read_from_gold_parquet(table_name)
                
                # 2. Exporter vers PostgreSQL
                self.export_to_postgres(df, table_name)
            
            self.logger.info("PostgreSQL export completed!")
            
        except Exception as e:
            self.logger.error(f"PostgreSQL export failed: {e}")
            raise
    
    def run_export(self):
        """Pipeline complet en 2 étapes"""
        self.logger.info("Starting 2-STEP export pipeline")
        self.logger.info("HIVE SILVER → PARQUET HDFS → POSTGRESQL")
        
        try:
            # ÉTAPE 1: Agrégations
            self.run_aggregations()
            
            # ÉTAPE 2: Export PostgreSQL
            self.run_postgres_export()
            
            self.logger.info("Complete pipeline SUCCESS!")
            self.logger.info("Tables ready for API/viz:")
            self.logger.info("  - state_stats: Cartes par état")
            self.logger.info("  - time_patterns: Heatmaps temporelles")
            self.logger.info("  - weather_analysis: Charts météo")
            self.logger.info("  - city_hotspots: Maps géo avec points")
            self.logger.info("  === ML TABLES ===")
            self.logger.info("  - ml_severity_predictions: Prédictions de sévérité")
            self.logger.info("  - ml_risk_zones: Zones à risque clustering")
            self.logger.info("  - ml_hotspots: Hotspots géographiques ML")
            
            # Afficher chemins HDFS pour vérification
            self.logger.info("GOLD Parquet datamarts:")
            for table in ["state_stats", "time_patterns", "weather_analysis", "city_hotspots", "ml_severity_predictions", "ml_risk_zones", "ml_hotspots" ]:
                gold_path = Config.get_gold_path(table)
                self.logger.info(f"  - {gold_path}")
            
        except Exception as e:
            self.logger.error(f"Pipeline failed: {e}")
            raise
     
    def close(self):
        if self.spark:
            self.spark.stop()
            self.logger.info("DataExporter closed")