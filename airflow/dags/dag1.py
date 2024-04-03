from airflow import DAG 
from airflow.operators.python_operator import PythonOperator 
from datetime import datetime 


def hi_world():
        return print('hi, world')

with DAG(
          dag_id='animals_project',
          start_date = datetime(2024, 4, 3),
          schedule_interval='0 20 * * *',
          catchup=False
) as dag:

          hi = PythonOperator(
                  task_id = 'test_task',
                  python_callable=hi_world
          )
          

hi