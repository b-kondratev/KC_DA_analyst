from datetime import datetime, timedelta
import pandas as pd
import pandahouse as ph
import os

from airflow.decorators import dag, task

default_args = {
    'owner': 'b.kondratev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 12, 8),
}

schedule_interval = '0 23 * * *'

def create_table():
    connection = {
        'host': 'https://clickhouse.lab.karpov.courses',
        'password': os.environ['KC_PASSWORD_TEST'],
        'user': 'student-rw',
        'database': 'test'
        }
    q = '''
    CREATE TABLE IF NOT EXISTS {db}.bkondratev_6(
    event_date date,
    dimension varchar(10),
    dimension_value varchar(10),
    views int,
    likes int,
    messages_received int,
    messages_sent int,
    users_received int,
    users_sent int)
    ENGINE = Log'''
    ph.execute(q, connection=connection)


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_bkondratev():
    
    @task
    def extract_feed():
        connection = {
        'host': 'https://clickhouse.lab.karpov.courses',
        'password': os.environ['KC_PASSWORD'],
        'user': 'student',
        'database': 'simulator_20221120'
        }
        q = '''
        select user_id, 
        toDate(time) as event_date,
        os,
        gender,
        age,
        countIf(action='view') as views, 
        countIf(action='like') as likes
        from {db}.feed_actions
        where toDate(time) = yesterday()
        group by user_id, event_date, os, gender, age
        '''
        df_feed = ph.read_clickhouse(q, connection=connection)
        return df_feed
    
    @task
    def extract_messages():
        connection = {
        'host': 'https://clickhouse.lab.karpov.courses',
        'password': os.environ['KC_PASSWORD'],
        'user': 'student',
        'database': 'simulator_20221120'
        }
        q1 = '''
        select user_id,
        toDate(time) as event_date,
        os,
        gender,
        age,
        count(*) as messages_sent,
        uniq(reciever_id) as users_sent
        from {db}.message_actions
        where toDate(time) = yesterday()
        group by user_id, event_date, os, gender, age'''
        q2= '''   
        select reciever_id,
        toDate(time) as event_date,
        count(*) as messages_received,
        uniq(user_id) as users_received
        from {db}.message_actions
        where toDate(time) = yesterday()
        group by reciever_id, event_date
        '''
        df_messages1 = ph.read_clickhouse(q1, connection=connection)
        df_messages2 = ph.read_clickhouse(q2, connection=connection)
        df_messages2.rename(columns={'reciever_id':'user_id'}, inplace=True)
        df_messages = df_messages1.merge(df_messages2, on=['user_id', 'event_date'], how='outer') #делаем через merge здесь потому что при inner join в sql-запросе теряются пользователи,
        df_messages.fillna(0, inplace=True)                                                       #получавшие сообщения но не отправлявшие их, а при full join дальнейшая работа с таблицей сильно затруднена из-за избытка столбцов
        return df_messages                                                                                               
       
    
    @task
    def merge_df(df_feed, df_messages):
        df_comb = df_feed.merge(df_messages, on=['user_id', 'event_date', 'os', 'gender', 'age'], how='outer')
        df_comb.fillna(0, inplace=True)
        df_comb = df_comb[df_comb.os != 0] #дропаем пользователей, не пользовавшихся лентой и не отправлявших сообщений за вчерашний день, так как нам неизвестны их возраст, пол и ОС, в срезы вставить их не можем
        return df_comb
    
    @task
    def transform_os(df_comb):
        df_os = df_comb[['event_date', 'os', 'views', 'likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent']].groupby(['event_date', 'os'], as_index=False).sum()
        df_os.rename(columns={'os':'dimension_value'}, inplace=True)
        df_os.insert(1, 'dimension', 'os')
        return df_os
    
    @task
    def transform_gender(df_comb):
        df_gender = df_comb[['event_date', 'gender', 'views', 'likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent']].groupby(['event_date', 'gender'], as_index=False).sum()
        df_gender.replace({'gender':{0:'female', 1:'male'}}, inplace=True)
        df_gender.rename(columns={'gender':'dimension_value'}, inplace=True)
        df_gender.insert(1, 'dimension', 'gender')
        return df_gender
    
    @task
    def transform_age(df_comb):
        df_age = df_comb[['event_date', 'age', 'views', 'likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent']].groupby(['event_date', 'age'], as_index=False).sum()
        df_age.rename(columns={'age':'dimension_value'}, inplace=True)
        df_age.insert(1, 'dimension', 'age')
        return df_age
    
    @task
    def load(df_os, df_gender, df_age):
        connection = {
        'host': 'https://clickhouse.lab.karpov.courses',
        'password': os.environ['KC_PASSWORD_TEST'],
        'user': 'student-rw',
        'database': 'test'
        }
        df_total = pd.concat([df_os, df_gender, df_age])
        for col in ['views', 'likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent']:
            df_total = df_total.astype({col: 'int32'})
        ph.to_clickhouse(df_total, 'bkondratev_6', connection=connection, index=False)
    
    df_feed = extract_feed()
    df_messages = extract_messages()
    df_comb = merge_df(df_feed, df_messages)
    df_os = transform_os(df_comb)
    df_gender = transform_gender(df_comb)
    df_age = transform_age(df_comb)
    load(df_os, df_gender, df_age)
        
        
create_table()
dag_bkondratev = dag_bkondratev()