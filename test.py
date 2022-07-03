from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

def print_test():
    return 'Second DAG Test'

dag = DAG('test_Dag', description='test_Dag',
          schedule_interval='0 12 * * *',
          start_date=datetime(2017, 3, 20), catchup=False)

test_operator = PythonOperator(task_id='test_Dag', python_callable=print_test, dag=dag)

test_operator 
