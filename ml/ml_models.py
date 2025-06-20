import os
import sys
import time

# Ajouter le chemin parent pour les imports
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.regression import LinearRegression
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, RegressionEvaluator
from pyspark.ml import Pipeline

# Imports de ton architecture existante
from config.config import Config
from utils.logger import setup_logger
from utils.spark_utils import create_spark_session

class MLModels:
    def __init__(self):
        self.logger = setup_logger("MLModels")
        self.spark = create_spark_session("MLModels")
        self.logger.info("MLModels initialized with existing architecture")
        
        # Dossiers pour sauvegarder
        os.makedirs('./ml_results', exist_ok=True)
        
        self.start_time = None
        
    def start_timer(self):
        """Timer de performance"""
        self.start_time = time.time()
        self.logger.info("ML Training timer started")
        
    def end_timer(self):
        """Fin du timer"""
        duration = time.time() - self.start_time
        self.logger.info(f"ML Training completed in {duration:.2f} seconds")
        return duration
    
    def show_hive_schema(self):
        """Affiche le schéma de la table Hive pour debug"""
        self.logger.info("Checking Hive table schema...")
        
        try:
            # Afficher le schéma
            df_schema = self.spark.sql(f"DESCRIBE {Config.HIVE_DB}.accidents_silver")
            self.logger.info("Hive table schema:")
            df_schema.show(50, truncate=False)
            
            # Sample data
            sample_df = self.spark.sql(f"SELECT * FROM {Config.HIVE_DB}.accidents_silver LIMIT 3")
            self.logger.info("Sample data from Hive:")
            sample_df.show(3, truncate=False)
            
            return df_schema
            
        except Exception as e:
            self.logger.error(f"Error checking schema: {e}")
            return None

    def load_silver_data(self, limit=100000):
        """Charge les données depuis Hive Silver avec les VRAIS noms de colonnes"""
        self.logger.info(f"Loading Silver data from Hive (limit: {limit:,})")
        
        # Debug du schéma d'abord
        self.show_hive_schema()
        
        # Requête
        query = f"""
        SELECT 
            severity,
            start_lat, start_lng, end_lat, end_lng,
            distance_mi,
            temperature_f, humidity, pressure_in,
            visibility_mi, wind_speed_mph,
            weather_condition, wind_direction,
            amenity, bump, crossing, give_way, junction, 
            no_exit, railway, roundabout, station, stop, 
            traffic_calming, traffic_signal, turning_loop,
            sunrise_sunset,
            start_time
        FROM {Config.HIVE_DB}.accidents_silver 
        WHERE severity IS NOT NULL 
        AND start_lat IS NOT NULL 
        AND start_lng IS NOT NULL
        LIMIT {limit}
        """
        
        try:
            df = self.spark.sql(query)
            count = df.count()
            
            self.logger.info(f"Data loaded from Hive: {count:,} rows")
            self.logger.info(f"Columns: {len(df.columns)}")
            
            # Afficher échantillon
            self.logger.info("Sample data:")
            df.show(5, truncate=False)
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error loading data: {e}")
            self.logger.info("Trying basic fallback query...")
            
            # Fallback basique pour voir les colonnes
            try:
                basic_df = self.spark.sql(f"SELECT * FROM {Config.HIVE_DB}.accidents_silver LIMIT 10")
                self.logger.info("Available columns in table:")
                for col in basic_df.columns:
                    self.logger.info(f"  - {col}")
                basic_df.show(3)
                
                # Requête ultra basique
                if 'severity' in basic_df.columns:
                    minimal_df = self.spark.sql(f"""
                        SELECT severity, start_lat, start_lng 
                        FROM {Config.HIVE_DB}.accidents_silver 
                        WHERE severity IS NOT NULL 
                        LIMIT {limit}
                    """)
                    return minimal_df
                
            except Exception as e2:
                self.logger.error(f"Even basic query failed: {e2}")
            
            raise
    
    def prepare_ml_data(self, df):
        """Prépare les données pour ML avec les vrais noms de colonnes"""
        self.logger.info("Preparing ML data...")
        
        # Nettoyer les valeurs manquantes avec fillna
        fillna_dict = {}
        
        # Vérifier quelles colonnes existent
        available_columns = df.columns
        self.logger.info(f"Available columns: {available_columns}")
        
        # Colonnes numériques 
        numeric_mappings = {
            'distance_mi': 1.0,
            'temperature_f': 70.0,
            'humidity': 60.0,               
            'pressure_in': 29.92,
            'visibility_mi': 10.0,
            'wind_speed_mph': 5.0
        }
        
        for col, default_val in numeric_mappings.items():
            if col in available_columns:
                fillna_dict[col] = default_val
        
        # Colonnes catégorielles
        categorical_mappings = {
            'weather_condition': 'Clear',
            'wind_direction': 'CALM',
            'sunrise_sunset': 'Day'
        }
        
        for col, default_val in categorical_mappings.items():
            if col in available_columns:
                fillna_dict[col] = default_val
        
        # Colonnes booléennes
        boolean_cols = ['amenity', 'bump', 'crossing', 'give_way', 'junction', 
                       'no_exit', 'railway', 'roundabout', 'station', 'stop', 
                       'traffic_calming', 'traffic_signal', 'turning_loop']
        
        for col in boolean_cols:
            if col in available_columns:
                fillna_dict[col] = False
        
        self.logger.info(f"Filling nulls for {len(fillna_dict)} columns: {list(fillna_dict.keys())}")
        df_clean = df.fillna(fillna_dict)
        
        # Convertir booléens en int
        for col in boolean_cols:
            if col in available_columns:
                df_clean = df_clean.withColumn(col, F.col(col).cast("int"))
        
        # Filtrer valeurs aberrantes
        df_clean = df_clean.filter(
            (F.col("severity").between(1, 4)) &
            (F.col("start_lat").between(24.0, 50.0)) &
            (F.col("start_lng").between(-130.0, -65.0))
        )
        
        # Ajouter features temporelles si start_time existe
        if 'start_time' in available_columns:
            df_clean = df_clean.withColumn("hour", F.hour("start_time")) \
                             .withColumn("day_of_week", F.dayofweek("start_time"))
        
        count = df_clean.count()
        self.logger.info(f"ML data prepared: {count:,} rows")
        
        return df_clean
    
    def train_severity_model(self, df):
        """Modèle de prédiction de sévérité avec vrais noms de colonnes"""
        self.logger.info("\n SEVERITY PREDICTION MODEL")
        self.logger.info("="*50)
        
        # Features numériques de base
        numeric_features = ['start_lat', 'start_lng']
        
        # Ajouter features météo disponibles
        weather_features = ['distance_mi', 'temperature_f', 'humidity', 
                           'pressure_in', 'visibility_mi', 'wind_speed_mph']
        
        # Features booléennes infrastructure
        boolean_features = ['amenity', 'bump', 'crossing', 'give_way', 'junction', 
                           'traffic_signal', 'turning_loop']
        
        # Features temporelles
        temporal_features = ['hour', 'day_of_week']
        
        # Assembler toutes les features disponibles
        all_potential_features = numeric_features + weather_features + boolean_features + temporal_features
        available_features = [f for f in all_potential_features if f in df.columns]
        
        self.logger.info(f"Using {len(available_features)} features: {available_features}")
        
        if len(available_features) < 3:
            self.logger.error("Not enough features available")
            return None, 0
        
        # Pipeline ML
        assembler = VectorAssembler(
            inputCols=available_features,
            outputCol="features",
            handleInvalid="skip"
        )
        
        rf = RandomForestClassifier(
            featuresCol="features",
            labelCol="severity", 
            numTrees=50,
            maxDepth=8,
            seed=42
        )
        
        pipeline = Pipeline(stages=[assembler, rf])
        
        # Split train/test
        train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
        
        train_count = train_df.count()
        test_count = test_df.count()
        
        self.logger.info(f"Training: {train_count:,} samples")
        self.logger.info(f"Testing: {test_count:,} samples")
        
        if train_count < 100:
            self.logger.error("Not enough training data")
            return None, 0
        
        # Entraînement
        self.logger.info("Training Random Forest...")
        model = pipeline.fit(train_df)
        
        # Prédictions
        predictions = model.transform(test_df)
        
        # Évaluation
        evaluator = MulticlassClassificationEvaluator(
            labelCol="severity",
            predictionCol="prediction",
            metricName="accuracy"
        )
        
        accuracy = evaluator.evaluate(predictions)
        self.logger.info(f"Accuracy: {accuracy:.3f}")
        
        # Feature importance
        try:
            rf_model = model.stages[-1]
            if hasattr(rf_model, 'featureImportances'):
                importances = rf_model.featureImportances.toArray()
                self.logger.info("\n Top 5 Feature Importance:")
                
                feature_importance = list(zip(available_features, importances))
                feature_importance.sort(key=lambda x: x[1], reverse=True)
                
                for i, (feature, importance) in enumerate(feature_importance[:5]):
                    self.logger.info(f"  {i+1}. {feature}: {importance:.4f}")
        except Exception as e:
            self.logger.warning(f"Could not extract feature importance: {e}")
        
        # Sample predictions
        self.logger.info("\nSample Predictions:")
        predictions.select("severity", "prediction").show(10)
        
        # Confusion matrix
        self.logger.info("\nConfusion Matrix:")
        confusion_matrix = predictions.groupBy("severity", "prediction").count()
        confusion_matrix.orderBy("severity", "prediction").show()
        
        # Créer résultats pour Gold
        ml_results = predictions.select(
            "severity",
            "prediction", 
            "start_lat", 
            "start_lng"
        ).withColumn("model_type", F.lit("severity_prediction")) \
         .withColumn("accuracy", F.lit(accuracy)) \
         .withColumn("training_timestamp", F.current_timestamp())
        
        return ml_results, accuracy
    
    def create_risk_zones(self, df):
        """Clustering de zones à risque avec vrais noms"""
        self.logger.info("\n RISK ZONE CLUSTERING")
        self.logger.info("="*40)
        
        # Features pour clustering géographique
        geo_features = ['start_lat', 'start_lng']
        
        # Ajouter features contextuelles si disponibles
        context_features = []
        if 'hour' in df.columns:
            context_features.append('hour')
        if 'day_of_week' in df.columns:
            context_features.append('day_of_week')
        if 'junction' in df.columns:
            context_features.append('junction')
        if 'traffic_signal' in df.columns:
            context_features.append('traffic_signal')
        
        all_features = geo_features + context_features
        self.logger.info(f"Clustering with features: {all_features}")
        
        # Pipeline clustering
        assembler = VectorAssembler(
            inputCols=all_features,
            outputCol="features",
            handleInvalid="skip"
        )
        
        kmeans = KMeans(
            featuresCol="features",
            predictionCol="risk_zone",
            k=5,
            seed=42
        )
        
        pipeline = Pipeline(stages=[assembler, kmeans])
        
        # Entraînement
        self.logger.info("Training KMeans clustering...")
        model = pipeline.fit(df)
        clustered = model.transform(df)
        
        # Analyser les zones de risque
        risk_analysis = clustered.groupBy("risk_zone") \
            .agg(
                F.count("*").alias("accident_count"),
                F.avg("severity").alias("avg_severity"),
                F.avg("start_lat").alias("center_lat"),
                F.avg("start_lng").alias("center_lng")
            ) \
            .withColumn("risk_level", 
                F.when(F.col("avg_severity") >= 3, "HIGH")
                 .when(F.col("avg_severity") >= 2, "MEDIUM")
                 .otherwise("LOW")
            ) \
            .orderBy("risk_zone")
        
        self.logger.info("Risk Zone Analysis:")
        risk_analysis.show()
        
        return risk_analysis
    
    def create_hotspots(self, df):
        """Hotspots géographiques avec vrais noms"""
        self.logger.info("\n GEOGRAPHICAL HOTSPOTS")
        self.logger.info("="*35)
        
        # Créer grille géographique (vrais noms)
        hotspots = df.withColumn("lat_grid", F.round("start_lat", 1)) \
                    .withColumn("lng_grid", F.round("start_lng", 1)) \
                    .groupBy("lat_grid", "lng_grid") \
                    .agg(
                        F.count("*").alias("total_accidents"),
                        F.avg("severity").alias("avg_severity")
                    ) \
                    .filter(F.col("total_accidents") >= 5) \
                    .withColumn("hotspot_score", 
                        F.col("total_accidents") * F.col("avg_severity")
                    ) \
                    .orderBy(F.desc("hotspot_score")) \
                    .limit(50)
        
        hotspot_count = hotspots.count()
        self.logger.info(f"Created {hotspot_count} hotspots")
        hotspots.show(10)
        
        return hotspots
    
    def save_to_gold(self, df, model_name):
        """Sauvegarde les résultats ML en Gold"""
        gold_path = Config.get_gold_path(f"ml_{model_name}")
        self.logger.info(f"Saving {model_name} to Gold: {gold_path}")
        
        try:
            df.coalesce(2) \
              .write \
              .mode("overwrite") \
              .parquet(gold_path)
            
            count = df.count()
            self.logger.info(f"{model_name}: {count:,} rows saved to Gold")
            
        except Exception as e:
            self.logger.error(f"Error saving to Gold: {e}")
            # Fallback local
            df.coalesce(1) \
              .write \
              .mode("overwrite") \
              .parquet(f"./ml_results/{model_name}")
            self.logger.info(f"Saved locally: ./ml_results/{model_name}")
    
    def train_models(self):
        """Lance l'entraînement complet des modèles ML"""
        self.logger.info("Starting ML Training Pipeline")
        self.logger.info("="*50)
        
        self.start_timer()
        
        try:
            # 1. Charger données Silver depuis Hive
            df = self.load_silver_data(limit=150000)
            if df is None:
                raise Exception("Could not load data from Hive")
            
            # 2. Préparer données ML
            df_clean = self.prepare_ml_data(df)
            
            # 3. Dataset overview
            self.logger.info(f"\n DATASET OVERVIEW")
            self.logger.info("="*30)
            total_count = df_clean.count()
            self.logger.info(f"Total samples: {total_count:,}")
            
            # Distribution sévérité
            self.logger.info("\n Severity Distribution:")
            df_clean.groupBy("severity").count().orderBy("severity").show()
            
            results = {}
            
            # 4. MODÈLE 1: Prédiction de sévérité
            severity_results, accuracy = self.train_severity_model(df_clean)
            if severity_results is not None:
                self.save_to_gold(severity_results, "severity_predictions")
                results['severity_accuracy'] = accuracy
            
            # 5. MODÈLE 2: Zones à risque
            risk_zones = self.create_risk_zones(df_clean)
            self.save_to_gold(risk_zones, "risk_zones")
            results['risk_zones'] = risk_zones.count()
            
            # 6. MODÈLE 3: Hotspots
            hotspots = self.create_hotspots(df_clean)
            self.save_to_gold(hotspots, "hotspots")
            results['hotspots'] = hotspots.count()
            
            # 7. Résumé final
            duration = self.end_timer()
            
            self.logger.info("\n ML TRAINING COMPLETED!")
            self.logger.info("="*40)
            self.logger.info(f"Dataset: {total_count:,} samples processed")
            if 'severity_accuracy' in results:
                self.logger.info(f"Severity Prediction Accuracy: {results['severity_accuracy']:.3f}")
            self.logger.info(f"Risk Zones: {results.get('risk_zones', 0)} zones created")
            self.logger.info(f"Hotspots: {results.get('hotspots', 0)} hotspots identified")
            self.logger.info(f"Total Training Time: {duration:.2f} seconds")
            self.logger.info(f"Results saved in Gold layer")
            self.logger.info(f"Ready for DataMart export to PostgreSQL")
            
            # Afficher les chemins Gold
            self.logger.info("\n Gold Layer ML Results:")
            for model in ["severity_predictions", "risk_zones", "hotspots"]:
                gold_path = Config.get_gold_path(f"ml_{model}")
                self.logger.info(f"  - {gold_path}")
            
            return results
            
        except Exception as e:
            self.logger.error(f"ML training failed: {e}")
            import traceback
            traceback.print_exc()
            raise
        finally:
            self.cleanup()
    
    def cleanup(self):
        """Nettoyage des ressources"""
        if self.spark:
            self.spark.stop()
            self.logger.info("Spark session closed")

def main():
    """Point d'entrée principal"""
    print("Starting ML Models Training...")
    
    # Créer dossiers résultats
    os.makedirs("./ml_results", exist_ok=True)
    
    ml_trainer = MLModels()
    results = ml_trainer.train_models()
    
    print(f"ML Training completed!")
    print(f"Results: {results}")
    
    return results

if __name__ == "__main__":
    main()