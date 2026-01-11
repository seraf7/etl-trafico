from spark.spark_session import get_spark_session

from etl.extract.extract_stations import extract_stations
from etl.extract.extract_traffic import extract_traffic
from etl.extract.extract_weather import extract_weather

from load.load_to_hdfs import upload_to_hdfs, create_path_hdfs

from spark.transform_stations import transform_stations
from spark.transform_traffic import transform_traffic
from spark.transform_weather import transform_weather
from spark.transform_join import build_fact_table

print("Inicio proceso ETL")

# Extraccion de datos de APIs
print("Paso 1. Extraccion...")
estaciones_file = extract_stations()
trafico_file = extract_traffic()
clima_file = extract_weather()

# Carga de datos en HSDF
print("Paso 2. Carga en HSDF...")
HDFS_STATIONS = "/hdfs/traffic_weather/raw/stations"
HDFS_TRAFFIC = "/hdfs/traffic_weather/raw/traffic"
HDFS_WEATHER = "/hdfs/traffic_weather/raw/weather"
create_path_hdfs(HDFS_STATIONS)
create_path_hdfs(HDFS_TRAFFIC)
create_path_hdfs(HDFS_WEATHER)

upload_to_hdfs(estaciones_file, f"{HDFS_STATIONS}/stations.json")
upload_to_hdfs(trafico_file, f"{HDFS_TRAFFIC}/traffic.json")
upload_to_hdfs(clima_file, f"{HDFS_WEATHER}/weather.json")

# Transformaciones con Spark
print("Paso 4. Transformacion")

# Crea sesion de Spark
spark = get_spark_session()

# Aplica transformaciones
stations_df = transform_stations(spark)
traffic_df, dateDim = transform_traffic(spark)
weather_df = transform_weather(spark, dateDim)
fact_df = build_fact_table(stations_df, traffic_df, weather_df, dateDim)

# fact_df.show()

# Vista de resumen
print("Paso 5. Graba datos")

fact_df.write.mode("overwrite").partitionBy("year", "month", "day") \
    .parquet("hdfs://localhost:9000/hdfs/traffic_weather/processed/")

print("Fin del proceso")

