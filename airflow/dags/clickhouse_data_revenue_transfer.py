from master_functions import *
from airflow import DAG 
from airflow.operators.python_operator import PythonOperator  
import clickhouse_connect

def clickhouse_tranfser_to_data_function():
    client = clickhouse_connect.get_client(host='158.160.169.66', username='', password='') 
    inter_query = ''' SELECT idr.* FROM my_database.inter_data_revenue idr LEFT JOIN my_database.data_revenue dr ON idr.row_id = dr.row_id WHERE dr.row_id = 0'''
    result = client.query(inter_query)
    result_data = result.result_rows
    pandas_data = pd.DataFrame(result_data, columns=['row_id', 'order_id', 'order_date', 'status', 'source_path', 'user_id', 'promocode', 'item_id', 'country_id',
                                                     'category_id', 'revenue_raw', 'first_source','last_source','revenue'])
    database_name = 'my_database'
    table_name = 'data_revenue'
    client.insert_df(f'{database_name}.{table_name}', pandas_data)
    client.close()

with DAG(
          dag_id='clickhouse_dwh_transfer_data_revenue',
          start_date = datetime(2024, 6, 7),
          schedule_interval='30 15 * * *',
          catchup=False
) as dag:

          clickhouse_dwh_transfer_data_revenue_task = PythonOperator(
                  task_id = 'clickhouse_dwh_transfer_to_revenue_table',
                  python_callable=clickhouse_tranfser_to_data_function                  
          )
          
clickhouse_dwh_transfer_data_revenue_task 