from master_functions import *
from airflow import DAG 
from airflow.operators.python_operator import PythonOperator

def staging_status_update():
    """ Функция для совершения транзакции обновления статуса (staging) """
    connection = postgresql_engine()
    trans = connection.begin()

    result = connection.execute("SELECT staging.function_orders_status_update();").fetchone()
    print(result[0])

    trans.commit()
    connection.close()

with DAG(
          dag_id='call_function_pending_update_for_staging',
          start_date = datetime(2024, 6, 7),
          schedule_interval='20 14 * * *',
          catchup=False
) as dag:

          call_refresh_staging_orders = PythonOperator(
                  task_id = 'call_function_pending_update_for_staging_task',
                  python_callable=staging_status_update
          )
          
call_refresh_staging_orders    