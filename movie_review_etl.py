#
# Airflow DAG to create a Dataproc cluster, submit a Pyspark Job from GCS and
# destroy the cluster.
from airflow.utils.dates import days_ago
from airflow import DAG
from datetime import datetime
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator
)


GCP_CONN_ID = "google_cloud_conn"
GCS_BUCKET_NAME = "wizeline-project-356123-input"
GCS_KEY_NAME = "user_purchase.csv"

CLUSTER_NAME = 'bootcamp-cluster'
REGION="us-central1"
PROJECT_ID='wizeline-project-356123'
PYSPARK_URI='gs://wizeline-project-356123-input/movies_reviews_etl.py'

# Cluster definition
CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 50},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 50},
    },
}

PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_URI},
}

# Default arguments 
default_args = {
    'owner': 'Angel.Lopez',
    'depends_on_past': False,    
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False
}

with DAG(
    dag_id='submit_tokenizer',
    default_args=default_args,
    schedule_interval="@once",
    catchup=False,
    tags=['capstone']
    ) as dag:

    create_cluster = DataprocCreateClusterOperator(
        gcp_conn_id=GCP_CONN_ID,
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

    pyspark_task = DataprocSubmitJobOperator(
        gcp_conn_id=GCP_CONN_ID,
        task_id="pyspark_task",
        job=PYSPARK_JOB,
        region=REGION,
        project_id=PROJECT_ID
    )

    delete_cluster = DataprocDeleteClusterOperator(
        gcp_conn_id=GCP_CONN_ID,
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        trigger_rule='all_done'
    )

    create_cluster >> pyspark_task >> delete_cluster
    
