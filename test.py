from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator


DAG_ID = "gcp_database_ingestion_workflow"
CLOUD_PROVIDER = "gcp"
STABILITY_STATE = "unstable"

with DAG(
    dag_id = DAG_ID,
    schedule_interval = "@once",
    start_date=days_ago(1),
    tags = [CLOUD_PROVIDER,STABILITY_STATE]
) as dag:
    start_workflow = DummyOperator(task_id="start_workflow")
    
    start_workflow
