from pyspark.sql import SparkSession
from config.config import Config

def create_spark_session(app_name=None):
    return SparkSession.builder \
        .appName(app_name or Config.APP_NAME) \
        .config("spark.sql.catalogImplementation", "hive") \
        .config("spark.sql.warehouse.dir", Config.HIVE_WAREHOUSE) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .enableHiveSupport() \
        .getOrCreate()