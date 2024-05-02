import sqlalchemy
import psycopg2  
import pandas as pd
import numpy as np
import random
import time
from datetime import datetime, timedelta
from loguru import logger

def postgresql_engine():
    """ Функция подключения к БД """
    attempts = 1
    max_attempts = 3 + attempts
    while attempts < max_attempts:
        try:
            logger.info(f'Попытка подключения к БД номер {attempts}')
            engine = sqlalchemy.create_engine('postgresql://postgres:postgres123@158.160.159.20:5432/postgres')
            connect = engine.connect()
            return connect 
        except sqlalchemy.exc.OperationalError as e:
            logger.error(f'Ошибка sqlalchemy OperationalError: {e}')
            attempts += 1
            if attempts < max_attempts:
                logger.info(f'Ждем 3 минуты перед следующей попыткой...')
                time.sleep(180)
                logger.info(f'Пробуем попытку {attempts}')
            else:
                pass
        except Exception as e:      
            logger.error(f'Проблема с подключением: {e}') 
            attempts += 1
            if attempts < max_attempts:
                logger.info(f'Ждем 50 сек перед следующей попыткой...')
                time.sleep(50)
                logger.info(f'Пробуем попытку {attempts}')
            else:
                pass
            
def get_one_value_from_db(engine, sql_query):
    """ Функция возврата результата запроса к БД (1 значение) """
    with engine.connect() as connection:
        result = connection.execute(sql_query)
        total_one_value = result.scalar()
    return total_one_value   

def get_list_from_db(engine, sql_query):
    """ Функция возврата результата запроса к БД (список) """
    with engine.connect() as connection:
        result = connection.execute(sql_query)
        total_list = [row[0] for row in result.fetchall()]
    return total_list

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
    """ Запрос максимального user_id """
    max_user_from_master_orders = """select max(user_id) as max_user from master.orders"""
    """ Запрос максимального id заказа """
    max_order_id_from_master_orders = """select max(id) as max_id from master.orders"""
    """ Запрос списка user_id """
    user_id_list_for_returns = """select user_id 
                            from master.orders 
                            where order_date < current_date - interval '24 days' 
                            group by user_id order by user_id"""
    x_conn = postgresql_engine()    
    x_max_user = get_one_value_from_db(x_conn, max_user_from_master_orders)
    x_max_user = [x_max_user]
    x_max_order_id = get_one_value_from_db(x_conn, max_order_id_from_master_orders)
    x_max_order_id = [x_max_order_id]
    x_user_id_list = get_list_from_db(x_conn, user_id_list_for_returns)    
    
    def get_new_order_id():
        """ Функция генерации нового id заказа """  
        last_orders_id = x_max_order_id[-1]
        last_orders_id += 1
        x_max_order_id.append(last_orders_id)
        return last_orders_id  
    
    def get_new_user_id():
        """ Функция генерации нового id покупателя """ 
        choice = choose_random_user_type()
        last_user_id = x_max_user[-1]
        if choice == 'new':
            last_user_id += 1
            x_max_user.append(last_user_id)
            return last_user_id 
        elif choice == 'return':
            return random.choice(x_user_id_list)
    
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

    return orders_data, x_conn          

def download_to_master(func_dataset, table, schema):
    """ Функция записи датасета в таблицу БД """
    data, connect = func_dataset
    attempts_to_download = 1
    max_attempts_to_download = 3 + attempts_to_download
    while attempts_to_download < max_attempts_to_download:
        try:
            logger.info(f'Запись данные в БД, попытка номер {attempts_to_download}')
            data.to_sql(table, con = connect, schema = schema, if_exists = 'append', index = False)
            break
        except sqlalchemy.exc.OperationalError as ex:
            logger.error(f'Ошибка sqlalchemy OperationalError: {ex}')
            attempts_to_download += 1
            if attempts_to_download < max_attempts_to_download:
                logger.info(f'Ждем 3 минуты перед следующей попыткой записи...')
                time.sleep(180)
                logger.info(f'Пробуем попытку записи {attempts_to_download}')
            else:
                pass
        except Exception as ex:      
            logger.error(f'Проблема с записью данных: {ex}') 
            attempts_to_download += 1
            if attempts_to_download < max_attempts_to_download:
                logger.info(f'Ждем 70 сек перед следующей попыткой записи...')
                time.sleep(70)
                logger.info(f'Пробуем попытку записи {attempts_to_download}')
            else:
                pass    
    connect.close() 

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
    connect.close()    

def get_max_row_id_list_for_items():
    """ Функция получения максимального id строки для master.orders_items """ 
    connect = postgresql_engine()
    max_id = pd.read_sql_query('select max(id) from master.orders_items', con = connect)
    max_id = max_id['max'].max()
    return max_id

def get_order_ids_list_for_items():
    """ Функция получения id заказов, которых еще нет в master.orders_items """
    q_join = """select * from(
    select o.id as order_id
    from 
    master.orders o 
    left join master.orders_items oi 
    on o.id = oi.order_id
    where oi.order_id is null) as subq
    group by order_id"""     
    connect = postgresql_engine()
    order_ids_list = pd.read_sql_query(q_join, con = connect)
    order_ids_list = order_ids_list['order_id'].tolist()
    return order_ids_list

def get_orders_items_dataset(): 

    order_ids_list = get_order_ids_list_for_items() 

    def get_orders_column_for_orders_items():
        order_ids_list_for_items = []
        for order in order_ids_list:
            probability = random.random()
            if probability < 0.8:
                order_ids_list_for_items.append(order)
            elif probability < 0.91:
                order_ids_list_for_items.extend([order, order])
            else:
                order_ids_list_for_items.extend([order, order, order])
        return order_ids_list_for_items
    
    def get_items_column_for_orders_items():
        new_items = []
        order_ids_list_for_items = get_orders_column_for_orders_items()
        for order in order_ids_list_for_items:
            new_item = random.randint(1, 11)      
            new_items.append(new_item)
        return order_ids_list_for_items, new_items
    
    order_ids_list_for_items, new_items = get_items_column_for_orders_items()
    
    def get_row_id_column_for_items_orders():
        max_row_id = get_max_row_id_list_for_items() + 1        
        len_orders = len(order_ids_list_for_items)
        row_id_list = [x for x in range(max_row_id, max_row_id+len_orders)]
        return row_id_list
    
    row_id_list = get_row_id_column_for_items_orders()
    
    orders_items = pd.DataFrame({"order_id": order_ids_list_for_items, "item_id": new_items})
    orders_items = orders_items.drop_duplicates(subset=['order_id', 'item_id'])
    orders_items = orders_items.sort_values(by=['order_id', 'item_id'], ascending=True)
    orders_items.insert(0, 'id', row_id_list)
    return orders_items

def download_to_master_orders_items():
    connect = postgresql_engine()
    orders_items_data = get_orders_items_dataset()
    orders_items_data.to_sql('orders_items', con = connect, schema = 'master', if_exists = 'append', index = False)
    connect.close() 

def get_order_id_and_date_for_crm():
    q_join = """select mo.id, mo.order_date 
                from master.orders as mo 
                left join master.crm_rent crm on mo.id = crm.id
                where crm.id is null
                group by mo.id, mo.order_date"""     
    connect = postgresql_engine()
    order_ids_and_date = pd.read_sql_query(q_join, con = connect)
    return order_ids_and_date    

def crm_dataset_generation():
    
    data_crm = get_order_id_and_date_for_crm()
    
    def generate_random_start_days():
        weights_start_days = [0.2, 0.2, 0.15, 0.15, 0.1, 0.05, 0.05, 0.04, 0.03, 0.03] 
        choices_start_days = list(range(1, 11))
        random_start_days = random.choices(choices_start_days, weights=weights_start_days)[0] 
        return random_start_days

    def generate_random_end_days():
        weights_end = [0.1, 0.15, 0.15, 0.15, 0.1, 0.1, 0.05, 0.05, 0.05, 0.04, 0.03, 0.03] 
        choices_end = list(range(1, 13))
        random_end_days = random.choices(choices_end, weights=weights_end)[0] 
        return random_end_days

    data_crm['order_date'] = pd.to_datetime(data_crm['order_date'])
    data_crm['rent_start'] = data_crm['order_date'].apply(lambda x: x + timedelta(days=generate_random_start_days()))
    data_crm['rent_end'] = data_crm['rent_start'].apply(lambda x: x + timedelta(days=generate_random_end_days()))
    
    promocode_list = ['bloger_10', 'bloger_20', 'target_10', 'target_20', 'target_30', 'refer_10', 'refer_20']

    def generate_promocode():
        if random.random() < 0.77:  
            return np.nan
        else:
            return random.choice(promocode_list)
        
    data_crm['promocode'] = data_crm.apply(lambda x: generate_promocode(), axis=1) 
    
    return data_crm #test

def download_to_master_crm():
    connect = postgresql_engine()
    crm_rent_data = crm_dataset_generation()
    crm_rent_data.to_sql('crm_rent', con = connect, schema = 'master', if_exists = 'append', index = False)
    connect.close() 