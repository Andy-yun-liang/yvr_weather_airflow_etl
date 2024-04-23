
import requests
from airflow.models import Variable


def download_weather_data(ti):
    APIKEY = Variable.get("OPENWEATHERAPI_KEY")
    url = f"http://api.openweathermap.org/data/2.5/forecast?lat=49.2609&lon=-123.114&appid={APIKEY}"
    response = requests.get(url)
    ti.xcom_push(key = "jsonData",value = response.json())