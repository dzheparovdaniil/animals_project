from master_functions import *
from airflow import DAG 
from airflow.operators.python_operator import PythonOperator  

with DAG(
          dag_id='master_crm_rent_generation',
          start_date = datetime(2024, 4, 30),
          schedule_interval='30 13 * * *',
          catchup=False
) as dag:

          crm_rent_download_to_master_task = PythonOperator(
                  task_id = 'crm_rent_download_to_master',
                  python_callable=download_to_master, 
                  op_kwargs={'func_dataset': crm_rent_dataset(), 'table': 'crm_rent', 'schema': 'master'}                  
          )
          
crm_rent_download_to_master_task