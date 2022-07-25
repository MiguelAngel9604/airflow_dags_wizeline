import psycopg2
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago

temporary_file = 'temp_file.csv'

GCS_BUCKET = "wizeline-project-356123-input"
FILENAME = "csv_delete.csv"

DAG_ID = "postgres_to_gcs"
CLOUD_PROVIDER = "gcp"
STABILITY_STATE = "unstable"

GCP_CONN_ID = "google_cloud_conn"

# Postgres constants
POSTGRES_CONN_ID = "postcon"
POSTGRES_TABLE_NAME = "users_purchase"

def copy_hook():
    pg_hook = PostgresHook(POSTGRES_CONN_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.copy_expert("COPY dbname.users_purchase TO STDOUT WITH CSV HEADER", FILENAME)

    gcs_hook = GCSHook(GCP_CONN_ID)
    gcs_hook.upload(
        bucket_name=GCS_BUCKET,
        object_name=FILENAME,
        filename=temporary_file,
        timeout=120
    )

# Default arguments 
default_args = {
    'owner': 'Angel.Lopez',
    'depends_on_past': False,    
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval="@once",
    catchup=False,
    tags=['capstone']
    ) as dag:

    postgres_to_bucket = PythonOperator(
        task_id='postgres_to_bucket',
        provide_context=True,
        python_callable=copy_hook,
    )
