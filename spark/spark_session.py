from pyspark.sql import SparkSession

def get_spark_session():
    return SparkSession.builder \
    .master("local") \
    .appName("TrafficWeatherETL") \
    .config("spark.sql.parquet.compression.codec", "snappy") \
    .enableHiveSupport() \
    .getOrCreate()
