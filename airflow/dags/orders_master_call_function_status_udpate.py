from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime

dag = DAG(
    dag_id='call_function_pending_update',
    schedule_interval='10 13 * * *',  
    start_date=datetime(2024, 4, 25),
    catchup=False
)

call_postgres_function_refrash_status = PostgresOperator(
    task_id='function_refrash_status',
    postgres_conn_id='animals_postgres', 
    sql="SELECT f_orders_status_update()",
    dag=dag
)

call_postgres_function_refrash_status