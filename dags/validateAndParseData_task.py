from pydantic import ValidationError
from datetime import datetime
from logging import log
from pydantic import BaseModel
from typing import Optional, List



class Main(BaseModel):
    temp: Optional[float] 
    feels_like: Optional[float] 
    temp_min: Optional[float] 
    temp_max: Optional[float]
    pressure: Optional[float]
    sea_level: Optional[int] 
    grnd_level: Optional[int] 
    humidity: Optional[int] 
    temp_kf: float


class Weather(BaseModel):
    id: int 
    main: str 
    description: str 
    icon: str


class Clouds(BaseModel):
    all: int

class Wind(BaseModel):
    speed: float
    deg: int 
    gust: float

class Sys(BaseModel):
    pod: str 


class ListItem(BaseModel):
    dt: int
    main: Main 
    weather: List[Weather]
    clouds: Clouds 
    wind: Wind 
    visibility: int 
    pop: float 
    sys: Sys 
    dt_txt: str 

class Coord(BaseModel):
    lat: float 
    lon: float
    
class City(BaseModel):
    id: int 
    name: str 
    coord: Coord 
    country: str 
    population: int 
    timezone: int 
    sunrise: int
    sunset: int

class WeatherData(BaseModel):   
    cod: str
    message: int 
    cnt: int 
    list: List[ListItem]
    city: City


def validate_parse_data(ti):

    json_data = ti.xcom_pull(key = "jsonData",task_ids = 'download_weather_data')

    try:
       
        validateJsonData = WeatherData(** json_data)
        
    except ValidationError as e:
        log(e.json())
        
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
                'temp' : data.main.temp,
                'feels_like' : data.main.feels_like,
                'humidity' : data.main.humidity,
                'wind speed' : data.wind.speed

            }
            subsetDataList.append(subsetData)


        ti.xcom_push(key="parsedData",value = subsetDataList)
                     
