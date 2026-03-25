from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Add the scripts folder to the path so Airflow can find your code
sys.path.append('/opt/airflow')
from scripts import extract, transform

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 22),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'worldbank_economic_pipeline',
    default_args=default_args,
    description='Automated World Bank ETL with Airflow',
    schedule_interval='@daily',
    catchup=False
) as dag:

    # Task 1: Run Extraction
    extract_task = PythonOperator(
        task_id='extract_worldbank_data',
        python_callable=extract.run_pipeline,
    )

    # Task 2: Run Transformation
    transform_task = PythonOperator(
        task_id='transform_to_star_schema',
        python_callable=transform.run_transformations,
    )

    # Set dependencies (Extract then Transform)
    extract_task >> transform_task