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
    dict_f = {}
    for i in range(len(d["time"])):
        fecha = d["time"][i]
        if fecha not in dict_f:
            dict_f[fecha] = {}
            dict_f[fecha]["temperature_2m_mean"] = d["temperature_2m_mean"][i]
            dict_f[fecha]["rain_sum"] = d["rain_sum"][i]
            dict_f[fecha]["daylight_duration"] = d["daylight_duration"][i]
            dict_f[fecha]["snowfall_sum"] = d["snowfall_sum"][i]

    return dict_f

#Expansion del diccionario
def serie_clima(d):
    lista = []
    id_clima = 0
    i = 0
    while i in range(len(d)):
        dict_fechas = diccionario_fechas(d[i]["daily"])
        for fecha in dict_fechas:
            # Diccionario temporal
            dict_tmp = {}
            dict_tmp["id_clima"] = id_clima
            dict_tmp["latitude"] = d[i]["latitude"]
            dict_tmp["longitude"] = d[i]["longitude"]
            dict_tmp["fecha"] = fecha
            dict_tmp.update(dict_fechas[fecha])
            lista.append(dict_tmp)
            id_clima += 1
        i += 1
    return lista

def extract_weather():
    url = "https://archive-api.open-meteo.com/v1/archive?latitude=45.42,26.17,46.43,46.04,48.98,47.43&longitude=-105.4,-80.3,-117.95,-119.22,-119.45,-122.22&start_date=2022-01-01&end_date=2022-12-31&daily=temperature_2m_mean,apparent_temperature_mean,rain_sum,daylight_duration,sunrise,sunset,snowfall_sum"
    response = requests.get(url)
    data = response.json()

    # Eliminado de claves no relevantes
    columnas_borradas = ["daily_units", "generationtime_ms", "utc_offset_seconds", 
                         "timezone", "timezone_abbreviation", "elevation", "location_id"]
    for reg in data:
        for key in columnas_borradas: reg.pop(key, None)
        # reg["hourly"] = crea_serie(reg["hourly"])
    diccionario = serie_clima(data)

    filename = f"data/raw/weather/weather.json"
    with open(filename, "w") as f:
        #json.dump(data, f)
        json.dump(diccionario, f)

    return filename