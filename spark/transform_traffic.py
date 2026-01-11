from pyspark.sql.functions import *

def transform_traffic(spark):
    df = spark.read.json("hdfs://localhost:9000/hdfs/traffic_weather/raw/traffic/traffic.json")

    # Tabla de dimension de fecha
    dateDim = df.select(
        col("day"), 
        col("month"),
        col("year")
    )

    # Limpieza y normalizacion
    dateDim = dateDim \
    .withColumn("sha2",sha2(concat_ws("_", dateDim.day, dateDim.month, dateDim.year), 256)) \
    .distinct() \
    .withColumn("skeyDate", monotonically_increasing_id()) \
    .withColumn("tstamp", current_timestamp())

    # Limpieza de tabla de trafico
    df = df.join(dateDim, 
             (df.day == dateDim.day) 
             & (df.month == dateDim.month) 
             & (df.year == dateDim.year) 
             , "left")
    
    df = df \
    .drop("day", "month", "year", "sha2", "tstamp") \
    .withColumnRenamed("skeyDate", "id_dim_date") \
    .withColumn("state_cd", df["state_cd"].cast("long")) \
    .withColumn("sum_veh_count", df["sum_veh_count"].cast("long"))

    # Devuelve tabla de dimension de fecha y
    # tabla de trafico con FK
    return df, dateDim