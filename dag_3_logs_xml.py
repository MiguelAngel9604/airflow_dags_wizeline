import os
from datetime import datetime

from airflow import models
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable

ENV_ID = "dev"
DAG_ID = "dataproc_creation"
CLUSTER_NAME = f"cluster-dataproc-pyspark-{ENV_ID}"

ZONE = "us-central1-a"

BUCKET_NAME = "data-bootcamp-test-1-dev-data"
INIT_FILE = "Data_proc_scripts/pip-install.sh"

GCP_CONN_ID = "gcp_conn"

#VARIABLES
PROJECT_ID = Variable.get("PROJECT_ID")
GCS_BUCKET = Variable.get("STAGING_BUCKET")
FILENAME = "user_purchase.csv"
SQL_QUERY = "SELECT * FROM dbname.users_purchase;"
POSTGRES_CONN_ID = "postcon"
GCP_CONN_ID = "google_cloud_conn"
PYSPARK_FILE = "gs://etl_files_wz/logs_xml_el.py"
REGION = "us-west1"
# Cluster definition
# [START how_to_cloud_dataproc_create_cluster]


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
    "software_config": {
        "properties": {
            "spark:spark.jars.packages": "com.databricks:spark-xml_2.12:0.15.0"
        }
    }
}


PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_FILE},
}


with models.DAG(
    DAG_ID,
    schedule_interval='@once',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "dataproc"],
) as dag:
    # [START how_to_cloud_dataproc_create_cluster_operator]

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        gcp_conn_id=GCP_CONN_ID

    )
    # [END how_to_cloud_dataproc_create_cluster_operator]

    pyspark_task = DataprocSubmitJobOperator(
        task_id="pyspark_task",
        job=PYSPARK_JOB,
        region=REGION,
         project_id=PROJECT_ID,
        gcp_conn_id=GCP_CONN_ID
    )

    # [START how_to_cloud_dataproc_delete_cluster_operator]
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        gcp_conn_id=GCP_CONN_ID
    )
    # [END how_to_cloud_dataproc_delete_cluster_operator]
    delete_cluster.trigger_rule = TriggerRule.ALL_DONE

    create_cluster >> pyspark_task >> delete_cluster






