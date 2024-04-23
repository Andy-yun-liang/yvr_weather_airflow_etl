from sqlalchemy import create_engine
import logging
import pandas as pd



def upload_to_postgres(ti):
        
        parsed_data = ti.xcom_pull(key = "parsedData",task_ids = 'validate_and_parse_data')

        df = pd.DataFrame(parsed_data)

        engine = create_engine('postgresql://airflow:airflow@postgres/airflow')
        
        try:

            with engine.connect() as postgres_connection:
                    
                df.to_sql(con = engine, name = 'yvr_weather',if_exists='append')
                logging.info("Data has been uploaded")


        except Exception as e:
                
                logging.error("Error occured while uploading to Postgres:",e)
