import sqlalchemy
import psycopg2  
from master_functions import postgresql_engine
from datetime import datetime
from loguru import logger
from airflow import DAG 
from airflow.operators.python_operator import PythonOperator

def call_function_refresh():
    connection = postgresql_engine()
    sql_query = "SELECT master.f_orders_status_update();"
    connection.execute(sql_query)
    connection.close()

with DAG(
          dag_id='call_function_pending_update',
          start_date = datetime(2024, 4, 30),
          schedule_interval='20 13 * * *',
          catchup=False
) as dag:

          call_refresh_master_orders = PythonOperator(
                  task_id = 'call_function_pending_update_task',
                  python_callable=call_function_refresh
          )
          
call_refresh_master_orders 