import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
import pandahouse as ph
from datetime import datetime, date, timedelta
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

# параметры dag
default_args = {
    'owner': 'n-galina',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 9, 23),
}
schedule_interval = '0 11 * * *'

# подключение к бд
connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'database': 'simulator_20250820',
    'user': 'student',
    'password': '****'
}
my_token = 'здесь пишем токен бота'

# Отправляем в канал
chat_id = -100****

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def n_galina_full_report():

    @task()
    def get_feed_data():
        """
        Извлекаем данные из ClickHouse
        """
        q_1 = '''
        SELECT toDate(time) AS day,
               uniqExact(user_id) AS dau_feed,
               countIf(action='like') AS likes,
               countIf(action='view') AS views,
               countIf(action='like') / countIf(action='view') AS CTR
        FROM simulator_20250820.feed_actions
        WHERE toDate(time) BETWEEN toDate(now()) - 7 AND toDate(now()) - 1
        GROUP BY day
        ORDER BY day
        '''
        feed_data = ph.read_clickhouse(q_1, connection=connection)
        return feed_data
    
    @task()
    def get_message_data():
        q_2 = '''
        SELECT toDate(time) day,
               uniqExact(user_id) as dau_message,
               count (1) as messages_sent,
               count (1) / uniqExact(user_id) avg_messages_per_user
        FROM simulator_20250820.message_actions
        where toDate(time) BETWEEN toDate(now()) - 7 AND toDate(now()) - 1
        GROUP BY day
        ORDER BY day 
        '''
        message_data = ph.read_clickhouse(q_2, connection=connection)
        return message_data
    
    @task()
    def send_message(feed_data, message_data):
        yd = date.today() - timedelta(days=1)
        start_date = date.today() - timedelta(days=7)
        end_date = date.today() - timedelta(days=1)
        feed_data['dau_feed'] = feed_data['dau_feed'].astype(float)
        feed_data['likes'] = feed_data['likes'].astype(float)
        feed_data['views'] = feed_data['views'].astype(float)
        message_data['messages_sent'] = message_data['messages_sent'].astype(float)
        message_data['dau_message'] = message_data['dau_message'].astype(float)
        dau_f_percent_change = (feed_data.iloc[-1]['dau_feed'] - feed_data.iloc[-2]['dau_feed']) / feed_data.iloc[-2]['dau_feed'] * 100
        likes_percent_change = (feed_data.iloc[-1]['likes'] - feed_data.iloc[-2]['likes']) / feed_data.iloc[-2]['likes'] * 100
        views_percent_change = (feed_data.iloc[-1]['views'] - feed_data.iloc[-2]['views']) / feed_data.iloc[-2]['views'] * 100
        ctr_percent_change = (feed_data.iloc[-1]['CTR'] - feed_data.iloc[-2]['CTR']) / feed_data.iloc[-2]['CTR'] * 100
        dau_m_percent_change = (message_data.iloc[-1]['dau_message'] - message_data.iloc[-2]['dau_message']) / message_data.iloc[-2]['dau_message'] * 100
        messages_sent_percent_change = (message_data.iloc[-1]['messages_sent'] - message_data.iloc[-2]['messages_sent']) / message_data.iloc[-2]['messages_sent'] * 100
        messages_per_user_percent_change = (message_data.iloc[-1]['avg_messages_per_user'] - message_data.iloc[-2]['avg_messages_per_user']) / message_data.iloc[-2]['avg_messages_per_user'] * 100
    
        # Формирование сообщения
        msg = (
        f"📊 Отчёт по приложению за *{yd.strftime('%Y-%m-%d')}*:\n\n"
        f"Изменения относительно предыдущего дня\n"
        f"📰 *Лента*\n"
        f"• DAU: {feed_data.iloc[-1]['dau_feed']:.0f} ({dau_f_percent_change:.1f}%)\n"
        f"• Просмотры: {feed_data.iloc[-1]['views']:.0f} ({views_percent_change:.1f}%)\n"
        f"• Лайки: {feed_data.iloc[-1]['likes']:.0f} ({likes_percent_change:.1f}%)\n"
        f"• CTR: {feed_data.iloc[-1]['CTR']:.1%} ({ctr_percent_change:.1f}%)\n\n"
        f"💬 *Мессенджер*\n"
        f"• DAU: {message_data.iloc[-1]['dau_message']:.0f} ({dau_m_percent_change:.1f}%)\n"
        f"• Отправлено сообщений: {message_data.iloc[-1]['messages_sent']:.0f} ({messages_sent_percent_change:.1f}%)\n"
        f"• Среднее количество сообщений на пользователя: {message_data.iloc[-1]['avg_messages_per_user']:.1f} ({messages_per_user_percent_change:.1f}%)\n\n"
            )

        # Создание ГРАФИКА 1 (4 графика: Лента)
        plt.figure(figsize=[18, 14]) # Фигура 1
        plt.suptitle(f"""Метрики Ленты за неделю ({start_date.strftime('%Y-%m-%d')} - {end_date.strftime('%Y-%m-%d')})""", fontsize=19, fontweight='bold')
        # график 1
        plt.subplot(2, 2, 1)
        plt.plot(feed_data['day'], feed_data['dau_feed'],  label='DAU "Лента"', color='tab:gray', marker='o', linewidth=3)
        plt.grid()
        plt.xlabel("")
        plt.ylabel("")
        plt.title('DAU "Лента"', fontsize=15, fontweight='bold')

        # график 2
        plt.subplot(2, 2, 2)
        plt.plot(feed_data['day'], feed_data['views'], label='Просмотры', color='tab:gray', marker='o', linewidth=3)
        plt.grid()
        plt.xlabel("")
        plt.ylabel("")
        plt.title('Просмотры', fontsize=15, fontweight='bold')

        # график 3
        plt.subplot(2, 2, 3)
        plt.plot(feed_data['day'], feed_data['likes'], label='Лайки', color='tab:gray', marker='o', linewidth=3)
        plt.grid()
        plt.xlabel("")
        plt.ylabel("")
        plt.title('Лайки', fontsize=15, fontweight='bold')

        # график 4
        plt.subplot(2, 2, 4)
        plt.plot(feed_data['day'], feed_data['CTR'], label='CTR', color='tab:gray', marker='o', linewidth=3)
        plt.grid()
        plt.xlabel("")
        plt.ylabel("")
        plt.title('CTR', fontsize=15, fontweight='bold')
        
        # Сохраняем График 1
        plot_object_feed = io.BytesIO()
        plt.savefig(plot_object_feed)
        plot_object_feed.seek(0)
        plot_object_feed.name = 'report_feed.png'
        plt.close() # Закрываем фигуру 1
        
        # Создание ГРАФИКА 2 (2 графика: Мессенджер)
        plt.figure(figsize=[18, 8]) # Фигура 2
        plt.suptitle(f"""Метрики Мессенджера за неделю ({start_date.strftime('%Y-%m-%d')} - {end_date.strftime('%Y-%m-%d')})""", fontsize=19, fontweight='bold')
        # график 1
        plt.subplot(1, 2, 1)
        plt.plot(message_data['day'], message_data['dau_message'],  label='DAU "Мессенджер"', color='tab:gray',       marker='o', linewidth=3)
        plt.grid()
        plt.xlabel("")
        plt.ylabel("")
        plt.title('DAU "Мессенджер"', fontsize=15, fontweight='bold')

        # график 2
        plt.subplot(1, 2, 2)
        plt.plot(message_data['day'], message_data['avg_messages_per_user'], label='Среднее количество сообщений на пользователя', color='tab:gray', marker='o', linewidth=3)
        plt.grid()
        plt.xlabel("")
        plt.ylabel("")
        plt.title('Среднее количество сообщений на пользователя', fontsize=15, fontweight='bold')

        # Сохраняем График 2
        plot_object_message = io.BytesIO()
        plt.savefig(plot_object_message)
        plot_object_message.seek(0)
        plot_object_message.name = 'report_message.png'
        plt.close()

        return {
        'msg': msg,
        'plot_feed': plot_object_feed,
        'plot_message': plot_object_message
        }

    @task()
    def send_report(report_data):
        """
        Отправляем отчёт и график в Telegram.
        """
        # Распаковка словаря по ключам
        msg = report_data['msg']
        plot_feed = report_data['plot_feed']
        plot_message = report_data['plot_message']
 
        bot = telegram.Bot(token=my_token)

        bot.sendMessage(chat_id=chat_id, text=msg, parse_mode="Markdown")
        
        # Отправляем первую картинку (Лента)
        bot.sendPhoto(chat_id=chat_id, photo=plot_feed)
        
        # Отправляем вторую картинку (Мессенджер)
        bot.sendPhoto(chat_id=chat_id, photo=plot_message)

    # Определяем последовательность задач
    feed_data = get_feed_data()
    message_data = get_message_data()
    report_data = send_message(feed_data, message_data)
    send_report(report_data)

n_galina_full_report = n_galina_full_report()
