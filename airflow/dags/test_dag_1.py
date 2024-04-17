import pandas as pd
import random
import sqlalchemy
from airflow import DAG 
from airflow.operators.python_operator import PythonOperator 
from datetime import datetime

def postgresql_engine():
    engine = sqlalchemy.create_engine('postgresql://postgres:postgres123@158.160.159.20:5432/postgres')
    connect = engine.connect()
    return connect

def get_max_user_list():
    connect = postgresql_engine()
    max_user = pd.read_sql_query('select max(user_id) as max_user from master.orders', con = connect)
    max_user = max_user['max_user'].max()
    max_user_list = [max_user]
    return max_user_list

def get_max_order_id():
    connect = postgresql_engine()
    max_id = pd.read_sql_query('select max(id) as max_id from master.orders', con = connect)
    max_id = max_id['max_id'].max()
    max_id_list = [max_id]
    return max_id_list

def get_user_id_list():
    connect = postgresql_engine()
    q = "select user_id from master.orders where order_date < current_date - interval '24 days' group by user_id order by user_id"
    user_id_list = pd.read_sql_query(q, con = connect)
    user_id_list = user_id_list['user_id'].tolist()
    return user_id_list 

def custom_random_len_path():
    x = random.random()
    if x < 0.31:
        return 1
    elif x < 0.65:
        return 2
    else:
        return random.randint(3, 5)

def choose_random_source_path():
    source_list = ['google organic', 'yandex organic', 'direct', 'yandex-cpc', 'vk-cpc', 'referral', 'vk-social', 'other']
    weights = [0.2, 0.2, 0.2, 0.2, 0.1, 0.05, 0.03, 0.02]  
    num_elements = custom_random_len_path() 
    random_source_path = random.choices(source_list, weights = weights, k = num_elements) 
    return random_source_path

def choose_random_user_type():
    if random.random() > 0.73:             
        return 'return'                  
    else:
        return 'new'  
    
def global_dataset():
    
    max_user_list_b = get_max_user_list()
    user_id_list = get_user_id_list()
    max_order_list = get_max_order_id()

    def get_new_order_id():
        
        last_orders_id = max_order_list[-1]
        last_orders_id += 1
        max_order_list.append(last_orders_id)
        return last_orders_id    
    
    def get_new_user_id():

        choice = choose_random_user_type()
        last_user_id = max_user_list_b[-1]
        if choice == 'new':
            last_user_id += 1
            max_user_list_b.append(last_user_id)
            return last_user_id 
        elif choice == 'return':
            return random.choice(user_id_list)
        
    def row_gen():
        
        current_id = get_new_order_id()
        current_date = datetime.now().strftime('%Y-%m-%d')
        start_status = 'pending'
        source_path = choose_random_source_path()
        source_path =  '/'.join(source_path)
        user_id = get_new_user_id()
        order_row = [current_id, current_date, start_status, user_id, source_path]   
        return order_row  
    
    new_row_list = []
    counter = 0
    while counter < 7:
        new_row_list.append(row_gen())
        counter = counter + 1
    orders_data = pd.DataFrame(new_row_list, columns = ['id', 'order_date', 'status', 'user_id', 'source_path'])       

    return orders_data  

def download_to_master():
    connect = postgresql_engine()
    orders_data = global_dataset()
    orders_data.to_sql('orders', con = connect, schema = 'master', if_exists = 'append', index = False)

with DAG(
          dag_id='orders_generation_1_1',
          start_date = datetime(2024, 4, 13),
          schedule_interval='0 20 * * *',
          catchup=False
) as dag:

          orders_get_dataset_task = PythonOperator(
                  task_id = 'orders_get_dataset',
                  python_callable=global_dataset
          ), 
          orders_download_to_master_task = PythonOperator(
                  task_id = 'orders_download_to_master_1',
                  python_callable=download_to_master
          )

          
orders_get_dataset_task >> orders_download_to_master_task