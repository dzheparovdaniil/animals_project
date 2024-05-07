import sqlalchemy
import psycopg2  
from master_functions import postgresql_engine
from datetime import datetime
from loguru import logger
from airflow import DAG 
from airflow.operators.python_operator import PythonOperator
from master_functions import *

with DAG(
          dag_id='call_function_pending_update_test',
          start_date = datetime(2024, 4, 30),
          schedule_interval='20 13 * * *',
          catchup=False
) as dag:
              
          call_refresh_master_orders_TEST = PostgresOperator(
          task_id='run_postgres_query',
          sql="""SELECT * FROM master.orders LIMIT 1;""",
          postgres_conn_id='postgres_connection_test'
)
          
call_refresh_master_orders_TEST