from master_functions import *
from airflow import DAG 
from airflow.operators.python_operator import PythonOperator  

with DAG(
          dag_id='master_orders_items_generation',
          start_date = datetime(2024, 4, 30),
          schedule_interval='20 13 * * *',
          catchup=False
) as dag:

          orders_items_download_to_master_task = PythonOperator(
                  task_id = 'orders_items_download_to_master',
                  python_callable=download_to_master_orders_items
          )
          
orders_items_download_to_master_task