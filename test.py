from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.postgres.hooks.postgres import PostgresHook


DAG_ID = "gcp_database_ingestion_workflow"
CLOUD_PROVIDER = "gcp"
STABILITY_STATE = "unstable"

GCP_CONN_ID = "google_cloud_conn"
GCS_BUCKET_NAME = "wizeline-project-356123-input"
GCS_KEY_NAME = "user_purchase.csv"

# Postgres constants
POSTGRES_CONN_ID = "postcon"
POSTGRES_TABLE_NAME = "user_purchase"


def ingest_data_from_gcs (
    gcs_bucket: str,
    gcs_object: str,
    postgres_table: str,
    gcp_conn_id: str = "google_cloud_default",
    postgres_conn_id: str = "postgres_default",
):
    import tempfile
    
    gcs_hook = GCSHook(gcp_conn_id=gcp_conn_id)
    psql_hook = PostgresHook(postgres_conn_id)
    
    with tempfile.NamedTemporaryFile() as tmp:
        gcs_hook.download(
            bucket_name=gcs_bucket, object_name=gcs_object,filename=tmp.name
        )
        psql_hook.bulk_load(table=postgres_table, tmp_file=tmp.name)

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
    create_table_entity = PostgresOperator(
        task_id="PostgresOperator",
        postgres_conn_id =POSTGRES_CONN_ID,
        sql=f"""
        CREATE SCHEMA IF NOT EXISTS dbname;
        CREATE TABLE IF NOT EXISTS dbname.user_purchase (
            invoice_number VARCHAR(10),
            stock_code VARCHAR(20),
            detail VARCHAR(1000),
            quantity INT,
            invoice_date TIMESTAMP,
            unit_price NUMERIC(8,3),
            customer_id INT,
            country VARCHAR(20)
            );
        """
    )
    
    end_workflow = DummyOperator(
        task_id="end_workflow"
    )
    
    ingest_data = PythonOperator(
        task_id = "ingest_data",
        python_callable = ingest_data_from_gcs,
        op_kwargs={
            "gcp_conn_id": GCP_CONN_ID,
            "postgres_conn_id": POSTGRES_CONN_ID,
            "gcs_bucket": GCS_BUCKET_NAME,
            "gcs_object": GCS_KEY_NAME,
            "postgres_table": POSTGRES_TABLE_NAME,
        },
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )
    
    (
    start_workflow
    >> verify_key_existence
    >> create_table_entity
    >> ingest_data
    )
