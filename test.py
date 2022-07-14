from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor

DAG_ID = "gcp_database_ingestion_workflow"
CLOUD_PROVIDER = "gcp"
STABILITY_STATE = "unstable"

GCP_CONN_ID = "google_cloud_default"
GCS_BUCKET_NAME = "wizeline-project-356123-input"
GCS_KEY_NAME = "user_purchase.csv"

with DAG(
    dag_id = DAG_ID,
    schedule_interval = "@once",
    start_date=days_ago(1),
    tags = [CLOUD_PROVIDER,STABILITY_STATE]
) as dag:
    start_workflow = DummyOperator(task_id="start_workflow")
    
    verify_key_existence = GCSObjectExistenceSensor(
        task_id="verify_key_existence",
        google_cloud_conn_id=GCP_CONN_ID,
        bucket=GCS_BUCKET_NAME,
        object=GCS_KEY_NAME,
    )
    (
    start_workflow
    >> verify_key_existence
    )
