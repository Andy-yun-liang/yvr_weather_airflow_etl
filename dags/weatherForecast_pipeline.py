from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python import PythonOperator
from fetchWeatherData_task import download_weather_data
from validateAndParseData_task import validate_parse_data
from uploadToPostgres_task import upload_to_postgres
from datetime import datetime, timedelta


default_args =  {
    'owner' : 'airflow',
    'email' : 'yf98.liang@gmail.com',
    'email_on_failure' : True,
    'email_on_retry' : True,
    'retries' : 1,
    'retry_delay' : timedelta(minutes = 5)
}


with DAG("weatherForecast_pipeline",
         start_date = datetime(2024,4,20),
         description = "a YVR weather forecast data pipeline",
         schedule_interval = "@daily",
         default_args = default_args,
         catchup=False) as dag:
    

    is_weatherAPI_available = HttpSensor(
        task_id = 'is_weatherAPI_available',
        http_conn_id = 'weatherAPI',
        endpoint = 'data/2.5/forecast?lat=49.2609&lon=-123.114&appid={{conn.weatherAPI.extra_dejson.api_key}}',
        response_check=lambda response: response.status_code == 200,
        poke_interval = 7,
        timeout = 20
    )

    downloadApiData = PythonOperator(
        task_id = 'download_weather_data',
        python_callable = download_weather_data
    )

    validateAndParse = PythonOperator(
        task_id = 'validate_and_parse_data',
        python_callable = validate_parse_data
    )

    uploadToPostgres = PythonOperator(
        task_id = 'uploaded_data_to_postgres',
        python_callable = upload_to_postgres
    )


is_weatherAPI_available >> downloadApiData >> validateAndParse >> uploadToPostgres


