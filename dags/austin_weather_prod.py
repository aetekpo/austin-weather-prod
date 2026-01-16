from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests, pandas as pd
from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas
import os


def ingest_austin_weather():
    # Your working ETL code
    api_key = os.getenv("OPENWEATHER_API_KEY")
    url = f"https://api.openweathermap.org/data/2.5/weather?q=Austin,US&appid={api_key}&units=imperial"

    data = requests.get(url).json()
    df = pd.DataFrame(
        [
            {
                "load_timestamp": pd.Timestamp.now(),
                "temp_f": data["main"]["temp"],
                "humidity": data["main"]["humidity"],
                "description": data["weather"][0]["description"],
            }
        ]
    )

    # Snowflake (uses Airflow connection)
    conn = connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse="COMPUTE_WH",
        database="WEATHER_DB",
        schema="PUBLIC",
    )
    write_pandas(conn, df, "WEATHER_DATA")
    conn.close()


dag = DAG(
    "austin_weather_prod",
    start_date=datetime(2026, 1, 16),
    schedule="0 6 * * *",  # Daily 6AM
    catchup=False,
)

run_etl = PythonOperator(
    task_id="ingest_weather", python_callable=ingest_austin_weather, dag=dag
)
