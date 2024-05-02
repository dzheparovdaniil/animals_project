from master_functions import *
from airflow import DAG 
from airflow.operators.python_operator import PythonOperator  

with DAG(
          dag_id='master_orders_generation',
          start_date = datetime(2024, 4, 25),
          schedule_interval='0 13 * * *',
          catchup=False
) as dag:

          orders_download_to_master_task = PythonOperator(
                  task_id = 'orders_download_to_master',
                  python_callable=download_to_master, 
                  op_kwargs={'func_dataset': master_orders_dataset(), 'table': 'orders', 'schema': 'master'}

          )

          check_order_missed_dates_task = PythonOperator(
                  task_id = 'func_check_order_missed_dates',
                  python_callable=check_order_missed_dates
          )
          
orders_download_to_master_task>>check_order_missed_dates_task