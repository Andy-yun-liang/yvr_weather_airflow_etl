import os 
import requests as r
from datetime import datetime
import pandas as pd
from pydantic import ValidationError
from validation_model.models import WeatherData

APIKEY = os.environ.get("OPENWEATHERAPI")

url = f"http://api.openweathermap.org/data/2.5/forecast?lat=49.2609&lon=-123.114&appid={APIKEY}"
response = r.get(url)

json_data = response.json()

try:
    print(type(json_data))
    validateJsonData = WeatherData(** json_data)
    print(type(validateJsonData))

except ValidationError as e:
    print(e.json())
    
else:

    subsetDataList = []
    cityInfo = validateJsonData.city

    for data in validateJsonData.list:
        
        subsetData = {
            'city_id' : cityInfo.id,
            'city_name' :cityInfo.name,
            'latitude' : cityInfo.coord.lat,
            'longitude' : cityInfo.coord.lon,
            'country' : cityInfo.country,
            'population' : cityInfo.population,
            'day' : datetime.fromtimestamp(data.dt).strftime("%m-%d-%Y"),
            'predicted_time' : datetime.fromtimestamp(data.dt).strftime("%H:%M:%S"),
            'temp (kelvin)' : data.main.temp,
            'feels_like (kelvin)' : data.main.feels_like,
            'humidity %' : data.main.humidity,
            'wind speed (meters/s)' : data.wind.speed,
            'weather' : data.weather

        }
        subsetDataList.append(subsetData)

        
weatherForecastDF = pd.DataFrame(subsetDataList)


# currentDate = datetime.now().strftime("%m-%d-%Y")

# print(currentDate)


# directory_path ="./json_data"

# if not os.path.exists(directory_path):
#     os.makedirs(directory_path)



# with open(f"{directory_path}/yvr_WeatherF_5d3h_{currentDate}.json","w") as file:
#     json.dump(json_data,file,indent=4)



