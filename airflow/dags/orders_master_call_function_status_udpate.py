from master_functions import *
from datetime import datetime
from loguru import logger
from airflow import DAG 
from airflow.operators.python_operator import PythonOperator
import psycopg2

def execute_sql_query_pending_update():
    
    connection = psycopg2.connect(
        dbname='postgres',
        user='demid',
        password='demid123',
        host='158.160.159.20',
        port='5432'
    )

    cursor = connection.cursor()
    sql_query = "SELECT master.function_orders_status_update();"
    cursor.execute(sql_query)
    connection.commit()

    result = cursor.fetchone()
    print(result)

    cursor.close()
    connection.close()

with DAG(
          dag_id='call_function_pending_update',
          start_date = datetime(2024, 4, 30),
          schedule_interval='20 13 * * *',
          catchup=False
) as dag:

          call_refresh_master_orders = PythonOperator(
                  task_id = 'call_function_pending_update_task',
                  python_callable=execute_sql_query_pending_update
          )
          
call_refresh_master_orders 