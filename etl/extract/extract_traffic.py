import requests
import json
from datetime import datetime

def extract_traffic():
    # url = "https://data.transportation.gov/resource/ytjj-yht4.json?$limit=50000&$offset=50000"
    url = "https://data.transportation.gov/resource/ytjj-yht4.json?$query=SELECT%0A%20%20%60year%60%2C%0A%20%20%60month%60%2C%0A%20%20%60day%60%2C%0A%20%20sum(%60veh_count%60)%2C%0A%20%20%60station_id%60%2C%0A%20%20%60state_cd%60%0AWHERE%0A%20%20caseless_one_of(%0A%20%20%20%20%60station_id%60%2C%0A%20%20%20%20%22P22AAA%22%2C%0A%20%20%20%20%22P09AAA%22%2C%0A%20%20%20%20%22P05AAA%22%2C%0A%20%20%20%20%22P6AAAA%22%2C%0A%20%20%20%20%220W-204%22%2C%0A%20%20%20%20%22979933%22%0A%20%20)%0AGROUP%20BY%20%60year%60%2C%20%60month%60%2C%20%60day%60%2C%20%60station_id%60%2C%20%60state_cd%60"
    response = requests.get(url)
    data = response.json()

    # Eliminado de claves no relevantes
    columnas_borradas = ["direction", "lane", "fsystem_cd"]
    for reg in data:
        # print(reg)
        for key in columnas_borradas: reg.pop(key, None)

    filename = f"data/raw/traffic/traffic_2022.json"
    with open(filename, "w") as f:
        json.dump(data, f)

    return filename
