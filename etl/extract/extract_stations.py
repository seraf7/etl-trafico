import requests
import json

def extract_stations():
    url = "https://services.arcgis.com/xOi1kZaI0eWDREZv/arcgis/rest/services/NTAD_Weigh_in_Motion_Stations/FeatureServer/0/query?where=1%3D1&outFields=latitude,longitude,functional_class,state,station_id,State_FIPS&outSR=4326&f=json"
    response = requests.get(url)
    data = response.json()
    
    # Recupera caracteristicas de estaciones
    stations = []
    for station in data["features"]:
        stations.append(station["attributes"])

    filename = "data/raw/stations/stations.json"
    with open(filename, "w") as f:
        json.dump(stations, f)

    return filename
