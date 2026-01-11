# Proceso ETL para el analsis de trafico en una ciudad

## Preparación del ambiente

Crear ambiente virtual y activarlo
```bash
python -m venv .venv
souce .venv/bin/activate
```

Instalar las dependencias del proyecto
```bash
pip install -r requierements.txt
```

Verifica que los servicios de hadoop se encuentran arriba con el domando jps. Se obtiene una salida similar a la siguiente
```bash
(.venv) etl-trafico$ jps
29570 Jps
3862 SecondaryNameNode
3626 DataNode
3482 NameNode
```

Crear los directorios para los hdfs con los comandos

```bash
hdfs dfs -mkdir -p /hdfs/traffic_weather/raw/stations
hdfs dfs -mkdir -p /hdfs/traffic_weather/raw/traffic
hdfs dfs -mkdir -p /hdfs/traffic_weather/raw/weather
```

## Ejecución del proceso

Para ejecutar el proceso, se ejecuta el script main.py del proyecto
```bash
python3 main.py
```

El proceso indica la fase en la que se encuentra durante cada momento de ejcución y envía mensje de finalización
```log
Inicio proceso ETL
Paso 1. Extraccion...
Paso 2. Carga en HSDF...
2026-01-11 00:04:00,482 ...
Paso 4. Transformacion
...
Paso 5. Graba datos
[Stage 14:>                                          0 + 1[Stage 14:>                                            0+1                                                                          
Fin del proceso
```

Al finalizar el proceso, se guarda la tabla de hechos generada en formato parquet en la ruta `/hdfs/traffic_weather/processed/`. Se puede comprobar con el siguiente comando

```
pc:~$ hdfs dfs -ls /hdfs/traffic_weather/processed/*
2026-01-11 00:30:44,212 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Found 12 items
drwxr-xr-x   - usr supergroup          0 2026-01-11 00:04 /hdfs/traffic_weather/processed/year=2022/month=1
drwxr-xr-x   - usr supergroup          0 2026-01-11 00:05 /hdfs/traffic_weather/processed/year=2022/month=10
drwxr-xr-x   - usr supergroup          0 2026-01-11 00:05 /hdfs/traffic_weather/processed/year=2022/month=11
drwxr-xr-x   - usr supergroup          0 2026-01-11 00:05 /hdfs/traffic_weather/processed/year=2022/month=12
drwxr-xr-x   - usr supergroup          0 2026-01-11 00:04 /hdfs/traffic_weather/processed/year=2022/month=2
...
```