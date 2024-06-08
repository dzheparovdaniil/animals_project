from master_functions import *
from airflow import DAG 
from airflow.operators.python_operator import PythonOperator  
import clickhouse_connect

def get_inter_data_revenue():
    connection = postgresql_engine()
    data_revenue = get_table_from_db(connection, "select * from staging.mv_revenue_data where order_date > current_date - interval '21 day'")
    connection.close()
    return data_revenue

def clickhouse_tranfser_function():
    client = clickhouse_connect.get_client(host='158.160.169.66', username='', password='') 
    data_revenue = get_inter_data_revenue()
    database_name = 'my_database'
    table_name = 'inter_data_revenue'
    client.query(f'TRUNCATE TABLE {database_name}.{table_name}')
    client.insert_df(f'{database_name}.{table_name}', data_revenue)
    client.close()

with DAG(
          dag_id='clickhouse_dwh_transfer',
          start_date = datetime(2024, 6, 7),
          schedule_interval='10 15 * * *',
          catchup=False
) as dag:

          clickhouse_dwh_transfer_task = PythonOperator(
                  task_id = 'clickhouse_dwh_transfer_to_inter_table',
                  python_callable=clickhouse_tranfser_function                  
          )
          
clickhouse_dwh_transfer_task    