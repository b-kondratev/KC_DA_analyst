from datetime import timedelta, datetime
import telegram
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
import pandahouse as ph
import os

from airflow.decorators import dag, task

default_args = {
    'owner': 'b.kondratev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 12, 14),
}

schedule_interval = '0 11 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def bkondratev_full_report():
    connection = {
        'host': 'https://clickhouse.lab.karpov.courses',
        'password': os.environ['KC_PASSWORD'],
        'user': 'student',
        'database': 'simulator_20221120'
        }
    
    @task
    def extract_users():
              
        q = '''select count(*) users, if(user_id = 0, t2.date, date) as date,
        case when t1.user_id!=0 and t2.user_id!=0 then 'both' when t1.user_id!=0 then 'feed' else 'messages' end app
        from (
        select distinct user_id, toDate(time) as date from simulator_20221120.feed_actions
        where toDate(time) between today() - 7 and yesterday()) t1 
        full join
        (select distinct user_id, toDate(time) as date from simulator_20221120.message_actions
        where toDate(time) between today() - 7 and yesterday()
        ) t2
        on t1.date = t2.date and t1.user_id = t2.user_id
        group by date, app'''
        
        df1 = ph.read_clickhouse(q, connection=connection)
        return df1
    
    @task
    def extract_metrics():
        
        q = '''select date, views, likes, messages from(
        select toDate(time) date,
        sum(action = 'view') views, 
        sum(action = 'like') likes
        from simulator_20221120.feed_actions
        where toDate(time) between today() - 7 and yesterday()
        group by date) t1
        join (
        select toDate(time) date,
        count(*) as messages
        from simulator_20221120.message_actions
        where toDate(time) between today() - 7 and yesterday()
        group by date) t2
        on t1.date = t2.date'''
        
        
        df2 = ph.read_clickhouse(q, connection=connection)
        
        return df2
    
    @task
    def make_report(df1, df2, chat=None):
        chat_id = chat or os.environ['CHAT_DM']
        my_token = os.environ['BOT_TOKEN']
        bot = telegram.Bot(token=my_token)
        
        df1['date'] = df1.date.dt.date
        df2['date'] = df2.date.dt.date
        y_users = df1.sort_values(['date', 'app'], ascending=False).iloc[0:2, :]
        y_actions = df2[df2.date == y_users.iloc[0, 1]].squeeze()
        yesterday_msg = 'Метрики за {}:\nВсего пользователей: {}\nПросмотров: {}\nЛайков: {}\nСообщений: {}'.format(y_users.iloc[0, 1], y_users.users.sum(), y_actions.views, y_actions.likes, y_actions.messages)
        bot.sendMessage(chat_id=chat_id, text=yesterday_msg)
        
        sns.set(rc={'figure.figsize':(11.4, 9)})
        sns.set_theme()
        fig, ax = plt.subplots(2, 1)
        def plot(data, y, ax, title=None, x='date'):
            sns.lineplot(data=data, x=x, y=y, ax=ax)
            ax.set_title(title)
            ax.set_xlabel('')
        
        plot(data=df1[df1.app=='feed'], y='users', ax=ax[0])
        plot(data=df1[df1.app=='messages'], y='users', ax=ax[0], title='DAU')
        ax[0].legend(['feed', 'messages'])
        plot(data=df1[df1.app=='both'], y='users', ax=ax[1], title='Пользовались обоими сервисами в один день')
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'users.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        
        fig, ax = plt.subplots()
        plot(data=df2, y='views', ax=ax)
        plot(data=df2, y='likes', ax=ax) 
        plot(data=df2, y='messages', ax=ax, title='Просмотры, лайки и сообщения') 
        ax.legend(['Views', 'Likes', 'Messages']) 
        ax.set_ylabel('')
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'metrics.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
    
    df1 = extract_users()
    df2 = extract_metrics()
    make_report(df1, df2, chat=os.environ['KC_CHAT_REPORT'])

bkondratev_full_report = bkondratev_full_report()
            