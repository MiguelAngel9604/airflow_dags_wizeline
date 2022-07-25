"""
DAG using PostgresToGoogleCloudStorageOperator.
"""
import os
from datetime import datetime

from airflow import models
from airflow.providers.google.cloud.transfers.postgres_to_gcs import (
    PostgresToGCSOperator,
)
from airflow.models import Variable
from airflow.utils.dates import days_ago


PROJECT_ID = Variable.get("PROJECT_ID")
GCS_BUCKET = Variable.get("STAGING_BUCKET")
FILENAME = "user_purchase.csv"
SQL_QUERY = "SELECT * FROM dbname.users_purchase;"
POSTGRES_CONN_ID = "postcon"
GCP_CONN_ID = "google_cloud_conn"

default_args = {
    'owner': 'Angel.Lopez',
    'depends_on_past': False,    
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False
}

with models.DAG(
    dag_id="postgres_to_gcs",
    default_args=default_args,
    schedule_interval="@once",  # Override to match your needs
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["postgres", "gcs"],
) as dag:
    upload_data = PostgresToGCSOperator(
        postgres_conn_id=POSTGRES_CONN_ID,
        google_cloud_conn_id=GCP_CONN_ID,
        task_id="postgres_to_gcs",
        sql=SQL_QUERY,
        bucket=GCS_BUCKET,
        filename=FILENAME,
        export_format="csv",
        gzip=False,
    )

    upload_data
