from master_functions import *
from airflow import DAG 
from airflow.operators.python_operator import PythonOperator  

with DAG(
          dag_id='staging_costs_generation',
          start_date = datetime(2024, 6, 13),
          schedule_interval='10 13 * * *',
          catchup=False
) as dag:

          costs_download_to_staging_task = PythonOperator(
                  task_id = 'costs_download_to_staging',
                  python_callable=download_to_postgres, 
                  op_kwargs={'func_dataset': staging_costs_dataset(), 'table': 'costs', 'schema': 'staging'}                  
          )
          
costs_download_to_staging_task