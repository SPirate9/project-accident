from feeder.incremental_feeder import IncrementalFeeder
from preprocessor.data_preprocessor import DataPreprocessor
from utils.logger import setup_logger
from config.config import Config
import subprocess

def check_hdfs_path_exists(path):
    """Vérifie si un chemin HDFS existe"""
    try:
        result = subprocess.run(['hdfs', 'dfs', '-test', '-d', path], 
                              capture_output=True, text=True)
        return result.returncode == 0
    except:
        return False

def main():
    logger = setup_logger("MainPipeline")
    logger.info("Starting accident analysis pipeline")
    
    # Vérifier si les données Bronze existent déjà (dernière date = toutes les données)
    latest_date = Config.SIMULATION_DATES[-1]  # "2025-01-03"
    bronze_path = Config.get_bronze_path(latest_date)
    bronze_exists = check_hdfs_path_exists(bronze_path)
    
    if bronze_exists:
        logger.info(f"Bronze data already exists at {bronze_path}, skipping feeder")
    else:
        # Step 1: Incremental Feeder
        logger.info("=== STEP 1: INCREMENTAL FEEDER ===")
        feeder = IncrementalFeeder()
        
        try:
            feeder.run_feeder_pipeline()
            logger.info("Feeder completed successfully")
            
        except Exception as e:
            logger.error(f"Error in feeder: {e}")
            return
            
        finally:
            feeder.close()
    
    # Step 2: Data Preprocessing (toujours exécuté pour nettoyer les données)
    logger.info("=== STEP 2: DATA PREPROCESSING ===")
    preprocessor = DataPreprocessor()
    
    try:
        preprocessor.run_preprocessing_pipeline()
        logger.info("Preprocessing completed successfully")
        
    except Exception as e:
        logger.error(f"Error in preprocessing: {e}")
        return
        
    finally:
        preprocessor.close()
    
    logger.info("Pipeline completed successfully")

if __name__ == "__main__":
    main()