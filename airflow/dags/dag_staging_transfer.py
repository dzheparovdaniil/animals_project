from master_functions import *
from airflow import DAG 
from airflow.operators.python_operator import PythonOperator  

def staging_orders_dataset():
    engine = postgresql_engine()
    min_id = """select coalesce(min(mo.id), (select max(id) + 1 from master.orders)) as min_id
                from master.orders mo left join staging.total_orders so on mo.id = so.id
                where so.id is null"""  
    last_val = get_one_value_from_db(engine, min_id)
    actual_master_orders_query = """select mo.id, mo.order_date, mo.status, 
                                    mo.user_id, mo.source_path, crm.rent_start, crm.rent_end,
                                    crm.rent_end - crm.rent_start AS rent_days, crm.promocode
                                    from master.orders mo
                                    left join master.crm_rent crm ON mo.id = crm.id
                                    where mo.id >= '{}' order by mo.id""".format(last_val)     
    orders_for_staging_data = get_table_from_db(engine, actual_master_orders_query)
    return orders_for_staging_data, engine 

def staging_items_dataset():
    engine = postgresql_engine()
    staging_query = """select oi.id, oi.order_id, oi.item_id, i.item_name, i.country_id,
                       i.price_per_day, i.total_price, i.category_id 
                       from master.orders_items oi left join master.items i on oi.item_id = i.id
                       where oi.order_id in (select oi_sub.order_id
                       from master.orders_items oi_sub 
                       left join staging.items stag on oi_sub.id = stag.id 
                       where stag.id is null group by oi_sub.order_id)"""
    
    items_for_staging_data = get_table_from_db(engine, staging_query)
    return items_for_staging_data, engine

with DAG(
          dag_id='staging_transfer',
          start_date = datetime(2024, 5, 7),
          schedule_interval='0 14 * * *',
          catchup=False
) as dag:

          orders_staging_task = PythonOperator(
                  task_id = 'orders_staging_transfer',
                  python_callable=download_to_master, 
                  op_kwargs={'func_dataset': staging_orders_dataset(), 'table': 'total_orders', 'schema': 'staging'}
          )

          items_staging_task = PythonOperator(
                  task_id = 'items_staging_transfer',
                  python_callable=download_to_master, 
                  op_kwargs={'func_dataset': staging_items_dataset(), 'table': 'items', 'schema': 'staging'}
          )
          
orders_staging_task>>items_staging_task
