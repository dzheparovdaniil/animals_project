from master_functions import *
from airflow import DAG 
from airflow.operators.python_operator import PythonOperator

def refresh_mat_view():
    connection = postgresql_engine()
    connection.execute("""REFRESH MATERIALIZED VIEW staging.mv_revenue_data;""")
    connection.close()

with DAG(
          dag_id='refresh_mat_view_staging',
          start_date = datetime(2024, 6, 13),
          schedule_interval='45 14 * * *',
          catchup=False
) as dag:

          call_refresh_staging_mat_view = PythonOperator(
                  task_id = 'call_refresh_staging_mat_view_task',
                  python_callable=refresh_mat_view
          )
          
call_refresh_staging_mat_view       