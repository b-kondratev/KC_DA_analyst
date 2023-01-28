from datetime import timedelta, datetime
import telegram
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
import numpy as np
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

schedule_interval = '*/15 * * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def bkondratev_alert():
    
    def check_anomaly(df, metric, a=3, n=5):
        #функция для проверки на аномальность по правилу трех сигм была слишком чувствительна,
        #поэтому меняем на метод проверки межквартильного размаха для n предшествующих 15-минуток
        
        df['q25'] = df[metric].rolling(n).quantile(0.25)
        df['q75'] = df[metric].rolling(n).quantile(0.75)
        df['iqr'] = df['q75'] - df['q25']
        df['up'] = df['q75'] + a * df['iqr']
        df['low'] = df['q25'] - a * df['iqr']
        df['up'] = df['up'].rolling(5, center=True, min_periods=1).mean()   #сглаживаем верхнюю
        df['low'] = df['low'].rolling(5, center=True, min_periods=1).mean() #и нижнюю границы
        if df[metric].iloc[-1] > df.up.iloc[-1] or df[metric].iloc[-1] < df.low.iloc[-1]:
            is_anomaly = 1
        else:
            is_anomaly = 0
        return is_anomaly, df
    
    def check_anomaly_2(df, metric, a=3, n=5):
        #функция проверяет значение текущей 15-минутки относительно n предыдущих 15-минуток за сегодня
        #и n 15-минуток за вчера (с серединой в той же 15-минутке что и текущая)
        
        df2 = pd.concat([df, df.shift(96-n//2).rename(columns=lambda x: x+'2')], axis=1)
        sigma = np.append(df2[metric].iloc[-n-1:-1].values, df2[f'{metric}2'].iloc[-n:].values).std()
        mean = np.append(df2[metric].iloc[-n-1:-1].values, df2[f'{metric}2'].iloc[-n:].values).mean()
        up = mean + a * sigma
        low = mean - a * sigma
        if df[metric].iloc[-1] > up or df[metric].iloc[-1] < low:
            is_anomaly_2 = 1
        else:
            is_anomaly_2 = 0
        return is_anomaly_2, df2
    
    def send_alert(df, metric, bot, chat_id, borders=True):
        #функция для формирования и отправки графика в случае обнаружения аномалии
        sns.set(rc={'figure.figsize':(11.4, 9)})
        sns.set_theme()
        plt.title(metric)
        if borders == True:
            sns.lineplot(data=df, x='ts', y=metric, label=metric)
            sns.lineplot(data=df, x='ts', y='up', label='up')
            sns.lineplot(data=df, x='ts', y='low', label='low')
        else:
            sns.lineplot(data=df, x='ts', y=metric, label='today')
            sns.lineplot(data=df, x='ts', y=f'{metric}2', label='yesterday', linestyle='--')
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = '{}.png'.format(metric)
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
    
    
    @task
    def extract_data():
        connection = {
        'host': 'https://clickhouse.lab.karpov.courses',
        'password': os.environ['KC_PASSWORD'],
        'user': 'student',
        'database': 'simulator_20221120'
        }

        q1 = '''select toStartOfFifteenMinutes(time) ts,
        uniqExact(user_id) as feed_users,
        sum(action='view') as views,
        sum(action='like') as likes,
        likes/views as ctr
        from simulator_20221120.feed_actions
        where time >= yesterday() and time < toStartOfFifteenMinutes(now())
        group by ts'''
        
        q2 = '''select toStartOfFifteenMinutes(time) ts,
        uniqExact(user_id) as message_users,
        count(*) as messages
        from simulator_20221120.message_actions
        where time >= yesterday() and time < toStartOfFifteenMinutes(now())
        group by ts
        order by ts'''
        
        data_feed = ph.read_clickhouse(q1, connection=connection)
        data_message = ph.read_clickhouse(q2, connection=connection)
        data = data_feed.merge(data_message, on='ts').sort_values('ts')
        return data
    
    @task
    def check_metrics(data, chat=None, n=5):
        
        dashboard = 'https://superset.lab.karpov.courses/superset/dashboard/2387/'
        chat_id = chat or os.environ['CHAT_DM']
        my_token = os.environ['BOT_TOKEN']
        bot = telegram.Bot(token=my_token)
        metrics_list = ['feed_users', 'message_users', 'views', 'likes', 'ctr', 'messages']
        
        for metric in metrics_list:
            df = data[['ts', metric]].copy()
            is_anomaly, df = check_anomaly(df, metric, n=n)
            
            if is_anomaly:
                msg = 'Метрика {}:\nТекущее значение: {:.2f}\nОтклонение от предыдущего значения: {:.2%}\nДашборд для анализа ситуации:\n{}'.format(metric, df[metric].iloc[-1], abs(1 - (df[metric].iloc[-1] / df[metric].iloc[-2])), dashboard)
                bot.sendMessage(chat_id=chat_id, text=msg)
                send_alert(df=df, metric=metric, bot=bot, chat_id=chat_id)
                
            else: #чтобы не слать 2 алерта проверяем отклонение за сегодня+вчера только если нет отклонения за сегодня
                df = data[['ts', metric]].copy() #копируем еще раз чтобы избавиться от добавленных столбцов
                is_anomaly_2, df = check_anomaly_2(df, metric, n=n)
                
                if is_anomaly_2:
                    msg = 'Метрика {}:\nТекущее значение: {:.2f}\nОтклонение от вчерашнего значения: {:.2%}\nДашборд для анализа ситуации:\n{}'.format(metric, df[metric].iloc[-1], abs(1 - (df[metric].iloc[-1] / df[f'{metric}2'].iloc[-n//2])), dashboard)
                    bot.sendMessage(chat_id=chat_id, text=msg) 
                    send_alert(df=df, metric=metric, bot=bot, chat_id=chat_id, borders=False) #из-за метода подсчета сигмы и среднего построить границы не получилось, вместо них строим данные за вчера
                    
    data = extract_data()
    check_metrics(data, chat=os.environ['KC_CHAT_ALERTS']) 

bkondratev_alert = bkondratev_alert()