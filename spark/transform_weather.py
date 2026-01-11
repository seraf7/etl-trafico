from spark.spark_session import get_spark_session
from pyspark.sql.functions import *
from pyspark.sql.types import *

def transform_weather(spark, dateDim):
    df_clima = spark.read.json("hdfs://localhost:9000/hdfs/traffic_weather/raw/weather/weather.json")

    df_clima_fecha = df_clima.join(
    dateDim, 
    (dayofmonth(df_clima.fecha) == dateDim.day) 
    & (month(df_clima.fecha) == dateDim.month) 
    & (year(df_clima.fecha) == dateDim.year) 
    , "left")

    df_clima_fecha = df_clima_fecha\
    .drop("fecha", "day", "month", "year", "sha2", "tstamp")\
    .withColumnRenamed("skeyDate", "id_dim_date")

    df_clima_fecha = df_clima.join(
    dateDim, 
    (dayofmonth(df_clima.fecha) == dateDim.day) 
    & (month(df_clima.fecha) == dateDim.month) 
    & (year(df_clima.fecha) == dateDim.year) 
    , "left"
)

    df_clima_fecha = df_clima_fecha\
        .drop("fecha", "day", "month", "year", "sha2", "tstamp")\
        .withColumnRenamed("skeyDate", "id_dim_date")

    df_avg_clima = df_clima_fecha.groupBy("id_dim_date").agg(
    avg("apparent_temperature").alias("AVG_temp"),
    avg("rain").alias("AVG_rain"),
    avg("relative_humidity_2m").alias("AVG_hmdty"),
)

    return df_avg_clima

