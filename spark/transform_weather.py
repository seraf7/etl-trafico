from spark.spark_session import get_spark_session
from pyspark.sql.functions import *
from pyspark.sql.types import *

def transform_weather2(spark, dateDim):
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


def transform_weather(spark):
    df_clima = spark.read.json("hdfs://localhost:9000/hdfs/traffic_weather/raw/weather/weather.json")

    # Tabla de dimension de fecha
    dateDim = df_clima.select(col("fecha"))
    dateDim = dateDim \
    .withColumn("day", dayofmonth(df_clima.fecha)) \
    .withColumn("month", month(df_clima.fecha)) \
    .withColumn("year", year(df_clima.fecha))

    # Limpieza y normalizacion
    dateDim = dateDim \
    .drop("fecha") \
    .withColumn("sha2",sha2(concat_ws("_", dateDim.day, dateDim.month, dateDim.year), 256)) \
    .distinct() \
    .withColumn("skeyDate", monotonically_increasing_id()) \
    .withColumn("tstamp", current_timestamp())

    # Limpieza de tabla de clima
    df_clima = df_clima.join(dateDim, 
                             (dayofmonth(df_clima.fecha) == dateDim.day) 
                             & (month(df_clima.fecha) == dateDim.month) 
                             & (year(df_clima.fecha) == dateDim.year), 
                             "left")

    df_clima = df_clima \
    .drop("day", "month", "year", "sha2", "tstamp") \
    .withColumnRenamed("skeyDate", "id_dim_date")

    df_avg_clima = df_clima.groupBy("id_dim_date").agg(
        avg("temperature_2m_mean").alias("AVG_temp"),
        avg("rain_sum").alias("AVG_rain"),
        avg("daylight_duration").alias("AVG_daylight"),
        avg("snowfall_sum").alias("AVG_snowfall"),
    )

    return df_avg_clima, dateDim
    