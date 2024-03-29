from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.sql import BranchSQLOperator

DAG_ID = 'dag_0_users_postgres'
CLOUD_PROVIDER = "gcp"
STABILITY_STATE = "unstable"

GCP_CONN_ID = "google_cloud_conn"
GCS_BUCKET_NAME = "capable-hangout-357804-input"
GCS_KEY_NAME = "user_purchase.csv"

# Postgres constants
POSTGRES_CONN_ID = "postcon"
POSTGRES_TABLE_NAME = "users_purchase"


def ingest_data_from_gcs (
    gcs_bucket: str,
    gcs_object: str,
    postgres_table: str,
    gcp_conn_id: str = "google_cloud_default",
    postgres_conn_id: str = "postgres_default",
):
    import tempfile
        # Download the file to a temporary directory and return a file handle
    gcs_hook = GCSHook(gcp_conn_id=gcp_conn_id)
    tmp_file = gcs_hook.provide_file(object_url='gs://capable-hangout-357804-input/user_purchase.csv')
    
    # Open Postgres Connection
    get_postgres_conn = PostgresHook(postgres_conn_id).get_conn()
    curr = get_postgres_conn.cursor()

    # Load csv into postgres
    with tmp_file as tmp:
        tmp.flush()
        with open(tmp.name) as fh:
            curr.copy_expert("COPY dbname.users_purchase FROM STDIN WITH CSV HEADER", fh)
            get_postgres_conn.commit()
    

default_args = {
    'owner': 'Angel.Lopez',
    'depends_on_past': False,    
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False
}

with DAG(
    dag_id = DAG_ID,
    default_args=default_args,
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
        CREATE TABLE IF NOT EXISTS dbname.{POSTGRES_TABLE_NAME} (
            invoice_number VARCHAR(100),
            stock_code VARCHAR(200),
            detail VARCHAR(10000),
            quantity INT,
            invoice_date TIMESTAMP,
            unit_price NUMERIC(8,3),
            customer_id INT,
            country VARCHAR(200)
            );
        """
    )
    
    clear_table = PostgresOperator(
        task_id="clear_table",
        postgres_conn_id =POSTGRES_CONN_ID,
        sql=f"DELETE FROM dbname.{POSTGRES_TABLE_NAME}"
    )
    
    end_workflow = DummyOperator(
        task_id="end_workflow"
    )
    
    ingest_data = PythonOperator(
        task_id = "ingest_data",
        python_callable = ingest_data_from_gcs,
        provide_context=True,
        op_kwargs={
            "gcp_conn_id": GCP_CONN_ID,
            "postgres_conn_id": POSTGRES_CONN_ID,
            "gcs_bucket": GCS_BUCKET_NAME,
            "gcs_object": GCS_KEY_NAME,
            "postgres_table": "dbname."+POSTGRES_TABLE_NAME,
        },
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )
    continue_process  = DummyOperator(task_id = "continue_process")
    
    validate_data = BranchSQLOperator(
        task_id="validate_data",
        conn_id =POSTGRES_CONN_ID,
        sql=f"SELECT COUNT(*) AS total_rows FROM dbname.{POSTGRES_TABLE_NAME}",
        follow_task_ids_if_false = [continue_process.task_id],
        follow_task_ids_if_true = [clear_table.task_id],
                
    )
    
    
    
    (
    start_workflow
    >> verify_key_existence
    >> create_table_entity
    >> validate_data
    )
    validate_data >> [clear_table,continue_process] >> ingest_data
    ingest_data >> end_workflow
