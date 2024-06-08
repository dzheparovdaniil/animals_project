from master_functions import *
from datetime import datetime
from airflow import DAG 
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import create_engine

def master_status_update():
    db_uri = 'postgresql://demid:demid123@158.160.169.66:5432/postgres'

    # объект подключения к базе данных
    engine = create_engine(db_uri)

    connection = engine.connect()
    trans = connection.begin()

    # Вызов функции staging.function_orders_status_update() и получение результата
    result = connection.execute("SELECT master.function_orders_status_update();").fetchone()

    # Вывод результата
    print(result[0])

    trans.commit()
    connection.close()

with DAG(
          dag_id='call_function_pending_update_for_master',
          start_date = datetime(2024, 4, 30),
          schedule_interval='20 13 * * *',
          catchup=False
) as dag:

          call_refresh_master_orders = PythonOperator(
                  task_id = 'call_function_pending_update_for_master_task',
                  python_callable=master_status_update
          )
          
call_refresh_master_orders 