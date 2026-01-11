from pyspark.sql.functions import col

def transform_stations(spark):
    df = spark.read.json("hdfs://localhost:9000/hdfs/traffic_weather/raw/stations/stations.json")

    return df.select(
        col("station_id"),
        col("State_FIPS"),
        col("state"),
        col("latitude"),
        col("longitude"),
        col("functional_class")
    )