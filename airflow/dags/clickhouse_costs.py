from master_functions import *
from airflow import DAG 
from airflow.operators.python_operator import PythonOperator  
import clickhouse_connect

def get_staging_costs():
    connection = postgresql_engine()
    data_costs = get_table_from_db(connection, "select * from staging.costs")
    connection.close()
    return data_costs

def clickhouse_costs_transfer_function():
    client = clickhouse_connect.get_client(host='158.160.169.66', username='', password='') 
    costs = get_staging_costs()
    database_name = 'my_database'
    table_name = 'costs'
    client.query(f'TRUNCATE TABLE {database_name}.{table_name}')
    client.insert_df(f'{database_name}.{table_name}', costs)
    client.close()

with DAG(
          dag_id='clickhouse_dwh_costs_transfer',
          start_date = datetime(2024, 6, 17),
          schedule_interval='15 15 * * *',
          catchup=False
) as dag:

          clickhouse_dwh_transfer_costs_task = PythonOperator(
                  task_id = 'costs_download_to_dwh',
                  python_callable=clickhouse_costs_transfer_function                  
          )
          
clickhouse_dwh_transfer_costs_task       