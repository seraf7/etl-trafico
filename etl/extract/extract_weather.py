import requests
import json
from datetime import datetime

def split_fecha(f):
    return f.split("T")[0]

# Calculo de promedios del dia
def calcula_promedios(d):
    dic_prom = {}
    total = d[0]
    for k,v in d[1].items():
        dic_prom[k] = v // total
    return dic_prom

# Funcion para crear un diccionario de medidas por fecha
def diccionario_fechas(d):
    dic_f = {}
    for i in range(len(d["time"])):
        fecha_s = split_fecha(d["time"][i])
        if fecha_s not in dic_f:
            dic_f[fecha_s] = [1, {}]
            dic_f[fecha_s][1]["apparent_temperature"] = d.get("apparent_temperature")[i]
            dic_f[fecha_s][1]["rain"] = d.get("rain")[i]
            dic_f[fecha_s][1]["relative_humidity_2m"] = d.get("relative_humidity_2m")[i]
            dic_f[fecha_s][1]["temperature_2m"] = d.get("temperature_2m")[i]
        else:
            dic_f[fecha_s][0] += 1
            dic_f[fecha_s][1]["apparent_temperature"] += d.get("apparent_temperature")[i]
            dic_f[fecha_s][1]["rain"] += d.get("rain")[i]
            dic_f[fecha_s][1]["relative_humidity_2m"] += d.get("relative_humidity_2m")[i]
            dic_f[fecha_s][1]["temperature_2m"] += d.get("temperature_2m")[i]
    for k,v in dic_f.items():
        dic_f[k] = calcula_promedios(v)
    return dic_f

#Expansion del diccionario
def serie_clima(d):
    lista = []
    id_clima = 0
    i = 0
    while i in range(len(d)):
        dict_fechas = diccionario_fechas(d[i]["hourly"])
        for fecha in dict_fechas:
            # Diccionario temporal
            dic_tmp = {}
            dic_tmp["id_clima"] = id_clima
            dic_tmp["latitude"] = d[i]["latitude"]
            dic_tmp["longitude"] = d[i]["longitude"]
            dic_tmp["fecha"] = fecha
            dic_tmp.update(dict_fechas[fecha])
            id_clima += 1
            lista.append(dic_tmp)
        i += 1
    return lista

def extract_weather():
    url = "https://archive-api.open-meteo.com/v1/archive?latitude=45.42,26.17,46.43,46.04,48.98,47.43&longitude=-105.4,-80.3,-117.95,-119.22,-119.45,-122.22&start_date=2022-01-01&end_date=2022-01-09&hourly=apparent_temperature,rain,weather_code,relative_humidity_2m,temperature_2m"
    response = requests.get(url)
    data = response.json()

    # Eliminado de claves no relevantes
    columnas_borradas = ["generationtime_ms", "timezone_abbreviation", "elevation", 
                         "utc_offset_seconds", "location_id"]
    for reg in data:
        for key in columnas_borradas: reg.pop(key, None)
        # reg["hourly"] = crea_serie(reg["hourly"])
    diccionario = serie_clima(data)

    filename = f"data/raw/weather/weather.json"
    with open(filename, "w") as f:
        #json.dump(data, f)
        json.dump(diccionario, f)

    return filename