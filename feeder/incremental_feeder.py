from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from config.config import Config
from utils.logger import setup_logger
from utils.spark_utils import create_spark_session

class IncrementalFeeder:
    def __init__(self):
        self.logger = setup_logger("IncrementalFeeder")
        self.spark = create_spark_session("IncrementalFeeder")
        self.logger.info("IncrementalFeeder initialized")
    
    def read_source_data(self):
        self.logger.info(f"Reading source data from {Config.SOURCE_FILE}")
        df = self.spark.read.option("header", "true").option("inferSchema", "true").csv(Config.SOURCE_FILE)
        count = df.count()
        self.logger.info(f"Source data loaded: {count} rows")
        return df
    
    def split_data_into_parts(self, df):
        self.logger.info("Splitting data into 3 equal parts")
        df1, df2, df3 = df.randomSplit([0.33, 0.33, 0.34], seed=42)
        counts = [df1.count(), df2.count(), df3.count()]
        self.logger.info(f"Data split completed: Part1={counts[0]}, Part2={counts[1]}, Part3={counts[2]}")
        
        return df1, df2, df3
    
    def add_situation_date_to_part(self, df_part, simulation_date):
        """Ajoute situation_date à une partie"""
        self.logger.info(f"Adding situation_date {simulation_date} to data part")
        df_with_date = df_part.withColumn("situation_date", F.lit(simulation_date))
        return df_with_date
    
    def save_individual_parts(self, df_parts):
        """Sauvegarde chaque partie individuellement avec sa date"""
        self.logger.info("Saving individual parts with situation_date")
        
        saved_parts = {}
        
        for i, (df_part, date_str) in enumerate(zip(df_parts, Config.SIMULATION_DATES)):
            self.logger.info(f"Saving part {i+1} for date {date_str}")
            
            # Ajouter situation_date à cette partie
            df_with_date = self.add_situation_date_to_part(df_part, date_str)
            
            # Optimisation
            df_optimized = df_with_date.repartition(5, "State").cache()
        
            # Chemin temporaire pour cette partie
            temp_path = f"{Config.BRONZE_PATH}/temp/{date_str.replace('-', '_')}"
            df_optimized.write.mode("overwrite").parquet(temp_path)
            
            count = df_optimized.count()
            self.logger.info(f"Part {i+1} saved: {count} rows to {temp_path}")
            
            # Stocker le DataFrame pour utilisation
            saved_parts[date_str] = df_optimized
        
        return saved_parts
    
    def build_incremental_bronze(self, saved_parts):
        """Construit les données Bronze de manière incrémentale"""
        self.logger.info("Building incremental bronze data")
        
        accumulated_data = None
        
        for i, date_str in enumerate(Config.SIMULATION_DATES):
            self.logger.info(f"Building bronze for day {i+1}: {date_str}")
            
            if i == 0:
                # Jour 1: seulement la première partie
                accumulated_data = saved_parts[date_str]
                self.logger.info(f"Day 1: Using only part for {date_str}")
                
            else:
                # Jour 2+: lire la veille + ajouter le jour courant
                previous_date = Config.SIMULATION_DATES[i-1]
                previous_bronze_path = Config.get_bronze_path(previous_date)
                
                self.logger.info(f"Reading previous day data from {previous_bronze_path}")
                df_previous = self.spark.read.parquet(previous_bronze_path)
                
                # Ajouter les données du jour courant
                accumulated_data = df_previous.union(saved_parts[date_str])
                self.logger.info(f"Day {i+1}: Combined previous + current part")
            
            # Optimisation finale
            accumulated_data = accumulated_data.repartition(10, "State").cache()
            
            # Sauvegarder en Bronze
            bronze_path = Config.get_bronze_path(date_str)
            accumulated_data.coalesce(5).write.mode("overwrite").parquet(bronze_path)
            
            count = accumulated_data.count()
            self.logger.info(f"Bronze saved for {date_str}: {count} rows to {bronze_path}")
            
            # Libérer le cache
            accumulated_data.unpersist()
    
    def cleanup_temp_data(self):
        """Nettoie les données temporaires"""
        self.logger.info("Cleaning up temporary data")
        try:
            import subprocess
            subprocess.run(['hdfs', 'dfs', '-rm', '-r', f"{Config.BRONZE_PATH}/temp"], 
                         capture_output=True)
            self.logger.info("Temporary data cleaned")
        except:
            self.logger.warning("Could not clean temporary data")
    
    def run_feeder_pipeline(self):
        self.logger.info("Starting incremental feeder pipeline")
        
        try:
            # 1. Lire les données source
            df = self.read_source_data()
            
            # 2. Split en 3 parties
            df_parts = self.split_data_into_parts(df)
            
            # 3. Sauvegarder chaque partie avec situation_date
            saved_parts = self.save_individual_parts(df_parts)
            
            # 4. Construire Bronze de manière incrémentale
            self.build_incremental_bronze(saved_parts)
            
            # 5. Nettoyer les données temporaires
            self.cleanup_temp_data()
            
            self.logger.info("Incremental feeder pipeline completed successfully")
            
        except Exception as e:
            self.logger.error(f"Error in feeder pipeline: {e}")
            raise
    
    def close(self):
        if self.spark:
            self.spark.stop()
            self.logger.info("IncrementalFeeder closed")