import sqlalchemy
import psycopg2  
import pandas as pd
import random
from datetime import datetime

def postgresql_engine():
    """ Функция подключения к БД (мастер-система) """
    try:
        engine = sqlalchemy.create_engine('postgresql://postgres:postgres123@158.160.159.20:5432/postgres')
        connect = engine.connect()
    except Exception:      
        print('Не удалось подключиться к БД')
        raise Exception
    return connect
    

def get_max_user_list():
    """ Функция получения максимального user_id для последующей генерации данных"""
    connect = postgresql_engine()
    max_user = pd.read_sql_query('select max(user_id) as max_user from master.orders', con = connect)
    max_user = max_user['max_user'].max()
    max_user_list = [max_user]
    return max_user_list

def get_max_order_id():
    """ Функция получения максимального id заказа для последующей генерации данных"""
    connect = postgresql_engine()
    max_id = pd.read_sql_query('select max(id) as max_id from master.orders', con = connect)
    max_id = max_id['max_id'].max()
    max_id_list = [max_id]
    return max_id_list

def get_user_id_list():
    """ Функция получения списка user_id для использования в качестве вернувшихся юзеров (повторных заказов) """
    connect = postgresql_engine()
    q = "select user_id from master.orders where order_date < current_date - interval '24 days' group by user_id order by user_id"
    user_id_list = pd.read_sql_query(q, con = connect)
    user_id_list = user_id_list['user_id'].tolist()
    return user_id_list 

def custom_random_len_path():
    """ Функция генерации рандомного количества источников данных от 1 до 5 """
    x = random.random()
    if x < 0.31:
        return 1
    elif x < 0.65:
        return 2
    else:
        return random.randint(3, 5)
    
def choose_random_source_path():
    """ Функция генерации рандомной цепочки источников """
    source_list = ['google organic', 'yandex organic', 'direct', 'yandex-cpc', 'vk-cpc', 'referral', 'vk-social', 'other']
    weights = [0.2, 0.2, 0.2, 0.2, 0.1, 0.05, 0.03, 0.02]  
    num_elements = custom_random_len_path() 
    random_source_path = random.choices(source_list, weights = weights, k = num_elements) 
    return random_source_path    

def choose_random_user_type():
    """ Функция выбора нового или вернувшегося покупателя """
    if random.random() > 0.73:             
        return 'return'                  
    else:
        return 'new' 

def master_orders_dataset():

    max_user_list_b = get_max_user_list()
    user_id_list = get_user_id_list()
    max_order_list = get_max_order_id()

    def get_new_order_id():
        """ Функция генерации нового id заказа """  
        last_orders_id = max_order_list[-1]
        last_orders_id += 1
        max_order_list.append(last_orders_id)
        return last_orders_id     

    def get_new_user_id():
        """ Функция генерации нового id покупателя """ 
        choice = choose_random_user_type()
        last_user_id = max_user_list_b[-1]
        if choice == 'new':
            last_user_id += 1
            max_user_list_b.append(last_user_id)
            return last_user_id 
        elif choice == 'return':
            return random.choice(user_id_list)
        
    def row_gen_master_orders():
        """ Функция генерации строки для master.orders """   
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
    total_rows = random.randint(2, 7)
    while counter < total_rows:
        new_row_list.append(row_gen_master_orders())
        counter = counter + 1
    orders_data = pd.DataFrame(new_row_list, columns = ['id', 'order_date', 'status', 'user_id', 'source_path'])       

    return orders_data

def download_to_master():
    connect = postgresql_engine()
    orders_data = master_orders_dataset()
    orders_data.to_sql('orders', con = connect, schema = 'master', if_exists = 'append', index = False)


def check_order_missed_dates():
    connect = postgresql_engine()    
    q = """with recursive cte as (
        select min(order_date) as min_date, max(order_date) as max_date
        from master.orders where order_date > '2024-04-01'
        union all
        select min_date + 1, max_date
        from cte
        where min_date + 1 <= max_date
    )
    select 
    cte.min_date, mo.order_date
    from cte left join master.orders mo
    on cte.min_date = mo.order_date
    where mo.order_date is null"""
    missed_dates = pd.read_sql_query(q, con = connect)
    unique_values = [date.strftime('%Y-%m-%d') for date in missed_dates['min_date'].unique()]
    if len(unique_values) > 0:  
        print(f"Количество пропущенных дат: {len(unique_values)}. Пропуски: {', '.join(unique_values)}")
    else:
        print('Нет пропущенных дат')