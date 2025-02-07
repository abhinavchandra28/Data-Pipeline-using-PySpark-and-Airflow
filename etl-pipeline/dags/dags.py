from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from etl import etl_pipeline  # Import your ETL function

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'etl_pyspark_pipeline',
    default_args=default_args,
    description='ETL Pipeline with PySpark and Airflow',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    run_etl = PythonOperator(
        task_id='run_etl_pipeline',
        python_callable=etl_pipeline,  # Call the ETL pipeline function
    )

    run_etl
