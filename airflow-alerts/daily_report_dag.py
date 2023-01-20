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
    'start_date': datetime(2022, 12, 12),
}

schedule_interval = '0 11 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_bkondratev_bot1():
    
    @task
    def report(chat=None):
        chat_id = chat or os.environ['CHAT_DM'] #отправляю себе в лс если чат не задан
        
        my_token = os.environ['BOT_TOKEN']
        bot = telegram.Bot(token=my_token)
        
        connection = {
        'host': 'https://clickhouse.lab.karpov.courses',
        'password': os.environ['KC_PASSWORD'],
        'user': 'student',
        'database': 'simulator_20221120'
        }

        
        q = '''select toDate(time) date,
        uniq(user_id) users,
        sum(action = 'view') views, 
        sum(action = 'like') likes,
        likes/views ctr
        from simulator_20221120.feed_actions
        where toDate(time) between today() - 7 and yesterday()
        group by date'''
        
        data = ph.read_clickhouse(q, connection=connection)
        data['date'] = data.date.dt.date #убираем время
        
        yesterday = data.sort_values('date', ascending=False).iloc[0, :] #выделяем ланные за вчера
        yesterday_msg = 'Метрики за {}:\nDAU: {}\nПросмотры: {}\nЛайки: {}\nCTR: {:.4}'.format(yesterday.date, yesterday.users, yesterday.views, yesterday.likes, yesterday.ctr)
        
        sns.set(rc={'figure.figsize':(11.4, 18)})
        sns.set_theme()
        fig, ax = plt.subplots(4, 1)
        def plot(data, ax, y, title, x='date'): #функция для построения графика
                sns.lineplot(data=data, x=x, y=y, ax=ax)
                ax.set_title(title)
                ax.set_xlabel('')
        
        bot.sendMessage(chat_id=chat_id, text=yesterday_msg)
        plot(data, y='users',  title='DAU', ax=ax[0])
        plot(data, y='views',  title='Views', ax=ax[1])
        plot(data, y='likes',  title='Likes', ax=ax[2])
        plot(data, y='ctr',  title='CTR', ax=ax[3])   
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'metrics.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
    
    report(chat=os.environ['KC_CHAT_REPORT'])
    
dag_bkondratev_bot1 = dag_bkondratev_bot1()