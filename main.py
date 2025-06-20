from feeder.incremental_feeder import IncrementalFeeder
from preprocessor.data_preprocessor import DataPreprocessor
from utils.logger import setup_logger
from config.config import Config

def main():
    logger = setup_logger("MainPipeline")
    logger.info("Starting complete accident analysis pipeline with 4 applications")
    logger.info("=" * 70)
    
    try:
        # === STEP 1: DATA FEEDING (BRONZE) ===
        logger.info("=== APPLICATION 1: DATA FEEDING (BRONZE) ===")
        feeder = IncrementalFeeder()
        feeder.run_feeder_pipeline() 
        feeder.close()
        logger.info("APPLICATION 1 COMPLETED: Feeder")
        
        # === STEP 2: DATA PREPROCESSING (SILVER) ===
        logger.info("=== APPLICATION 2: DATA PREPROCESSING (SILVER) ===")
        preprocessor = DataPreprocessor()
        preprocessor.run_preprocessing_pipeline()
        preprocessor.close()
        logger.info("APPLICATION 2 COMPLETED: Preprocessor")
        
        # === STEP 3: MACHINE LEARNING ===
        logger.info("=== APPLICATION 3: MACHINE LEARNING TRAINING ===")
        from ml.ml_models import MLModels
        
        ml_trainer = MLModels()
        ml_results = ml_trainer.train_models()
        logger.info("APPLICATION 3 COMPLETED: ML Training")
        
        # === STEP 4: DATAMART & EXPORT ===
        logger.info("=== APPLICATION 4: DATAMART & POSTGRESQL EXPORT ===")
        from datamart.datamart_builder import DataExporter
        
        exporter = DataExporter()
        exporter.run_export()
        exporter.close()
        logger.info("APPLICATION 4 COMPLETED: Datamart")
        
        # === PIPELINE SUMMARY ===
        logger.info("=" * 70)
        logger.info("COMPLETE PIPELINE EXECUTED SUCCESSFULLY")
        logger.info("=" * 70)
        logger.info("ARCHITECTURE SUMMARY:")
        logger.info("  APPLICATION 1: Feeder -> Bronze Layer (HDFS)")
        logger.info("  APPLICATION 2: Preprocessor -> Silver Layer (Hive)")
        logger.info("  APPLICATION 3: ML Training -> Models + Gold Layer")
        logger.info("  APPLICATION 4: Datamart -> PostgreSQL (API Ready)")
        logger.info("")
        logger.info("RESULTS:")
        logger.info(f"  ML Results: {ml_results}")
        logger.info("  Data ready for visualization in PostgreSQL")
        logger.info("  ML models trained and saved in GOLD layer")
        logger.info("  API available at: http://localhost:8000/docs")
        logger.info("")
        logger.info("YARN EXECUTION:")
        logger.info("  Check Yarn UI: http://localhost:8088")
        logger.info("  Check Spark UI: http://localhost:4040")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise

if __name__ == "__main__":
    main()
