import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import telegram
import pandahouse as ph
from datetime import date, datetime, timedelta
import io
import sys
import os

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

# Аргументы dag:
default_args = {
    'owner': 'n-galina',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 9, 26)
            }

# Проверка должна проводиться каждые 15 мин.
schedule_interval = '*/15 * * * *'

# Параметры подключения к БД
connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'database': 'simulator_20250820',
    'user': 'student',
    'password': '*****'
}

my_token = 'токен'

# Отправляем в канал
# chat_id = -96931***

# Отправляем в личный чат
chat_id = 66568***

 
# отслеживание аномалии с помощью метода межквартильного размаха
def check_anomaly(df, metric, a=5, n=4):  
    df['q25'] = df[metric].shift(1).rolling(n).quantile(0.25) # расчет первого квартиля
    df['q75'] = df[metric].shift(1).rolling(n).quantile(0.75) # расчет третьего квартиля
    df['iqr'] = df['q75'] - df['q25'] # межквартильный размах
    df['low'] = df['q25'] - a*df['iqr'] # вычисление нижней границы
    df['up'] = df['q75'] + a*df['iqr'] # вычисление верхней границы

    # сгладим границы
    df['up'] = df['up'].rolling(n, center=True, min_periods=1).mean()
    df['low'] = df['low'].rolling(n, center=True, min_periods=1).mean()


    # проверяем на аномалию
    if df[metric].iloc[-1] > df['up'].iloc[-1] or df[metric].iloc[-1] < df['low'].iloc[-1]:
        is_alert = 1
    else:
        is_alert = 0
    
    return is_alert, df
    
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def alert_dag_n_galina():
    
    @task()
    def get_data():
        # получаем данные по ленте и мессенджеру
        q = '''
            SELECT *
            FROM
            (SELECT
                toStartOfFifteenMinutes(time) AS ts, 
                toDate(time) AS day, 
                formatDateTime(ts, '%R') AS hm, 
                uniqExact(user_id) AS users_feed,
                countIf(user_id, action='view') AS views, 
                countIf(user_id, action='like') AS likes,
                100 * likes / views AS CTR
            FROM simulator_20250820.feed_actions
            WHERE ts >= today() - 1 AND ts < toStartOfFifteenMinutes(now()) 
            GROUP BY ts, day, hm
            ORDER BY ts, day, hm 
            ) AS feed_data

            FULL JOIN

            (SELECT
                toStartOfFifteenMinutes(time) AS ts, 
                toDate(time) AS day, 
                formatDateTime(ts, '%R') AS hm, 
                uniqExact(user_id) AS users_messenger,
                COUNT(user_id) AS messages
            FROM simulator_20250820.message_actions
            WHERE ts >= today() - 1 AND ts < toStartOfFifteenMinutes(now())
            GROUP BY ts, day, hm
            ORDER BY ts, day, hm 
            ) AS message_data
            USING ts, day, hm

        '''

        data = ph.read_clickhouse(q, connection=connection)
        data = data.fillna(0)
        return data
    
    @task()
    def run_alerts(data):
        # система алертов
        bot = telegram.Bot(token=my_token)
        # метрики
        metrics = ['users_feed', 'views', 'likes', 'CTR', 'users_messenger', 'messages']  


        for metric in metrics:
            df = data[['ts', 'day', 'hm', metric]].copy()
            if len(df) < 2:
                continue

            is_alert, df = check_anomaly(df, metric)

            if is_alert == 1:
                current_val = df[metric].iloc[-1]
                previous_val = df[metric].iloc[-2]
                time_alert = df.ts.iloc[-1]
                if previous_val != 0:
                    last_val_diff = abs(1 - (current_val/previous_val)) 
                else:
                    last_val_diff = 1.0

                # создаем сообщение
                msg = (f"🔥 Аномальное значение! 🔥\n"
                       f"Время: {time_alert}\n"
                       f"Метрика: {metric}\n"
                       f"Текущее значение: {current_val:.2f}\n"
                       f"Отклонение от предыдущего значения: {last_val_diff:.2%}\n"
                       f"Cсылка на дашборд: https://superset.lab.karpov.courses/superset/dashboard/7609/")

                # создание графика
                fig, ax = plt.subplots(figsize=(16, 8))
                plt.tight_layout()
                
                sns.lineplot(x=df['ts'], y=df[metric], data=df, color='blue', label=metric, ax=ax)
                sns.lineplot(x=df['ts'], y=df['up'], data=df, color='red', label='up', linestyle='--', ax=ax)
                sns.lineplot(x=df['ts'], y=df['low'], data=df, color='green', label = 'low', linestyle='--', ax=ax)

                ax.grid(True, linestyle='--', alpha=0.6) # Включаем сетку
                
                ax.set(xlabel='Время')  
                ax.set(ylabel=metric)
                ax.tick_params(axis='x', rotation=45)
                
                ax.set_title('{}'.format(metric))
                ax.set(ylim=(0, None))

                # Сохранение и отправка
                plot_object = io.BytesIO()
                fig.savefig(plot_object, bbox_inches='tight')
                plot_object.seek(0)
                plot_object.name = '{0}.png'.format(metric)
                plt.close(fig)

                bot.sendMessage(chat_id=chat_id, text=msg)
                bot.sendPhoto(chat_id=chat_id, photo=plot_object)
            
        return    

    data = get_data()
    run_alerts(data)
    
alert_dag_n_galina = alert_dag_n_galina()
