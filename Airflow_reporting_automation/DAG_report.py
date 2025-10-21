import telegram
import matplotlib.pyplot as plt
import io
import pandas as pd
import pandahouse as ph
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

# параметры dag
default_args = {
    'owner': 'n-galina',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 9, 20),
}
schedule_interval = '0 11 * * *'

# подключение к БД
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
def n_galina_report():

    @task()
    def extract_data():
        """
        Извлекаем данные из ClickHouse.
        """
        query = '''
        SELECT toDate(time) AS day,
            uniqExact(user_id) AS DAU,
            countIf(action='like') AS likes,
            countIf(action='view') AS views,
            countIf(action='like')/countIf(action='view') AS CTR
        FROM simulator_20250820.feed_actions
        WHERE toDate(time) BETWEEN toDate(now()) - 7 AND toDate(now()) - 1
        GROUP BY day
        ORDER BY day
        '''
        df_metrics = ph.read_clickhouse(query=query, connection=connection)
        return df_metrics

    @task()
    def transform_and_plot(df_metrics):
        """
        Создаём текст отчёта и график.
        """
        metrics_yesterday = df_metrics.iloc[-1]
        
        # Создание текста
        msg = (f"📊 Отчёт за *{metrics_yesterday['day'].strftime('%Y-%m-%d')}*\n"
               f"• DAU: *{metrics_yesterday['DAU']}*\n"
               f"• Просмотры: *{metrics_yesterday['views']}*\n"
               f"• Лайки: *{metrics_yesterday['likes']}*\n"
               f"• CTR: *{metrics_yesterday['CTR']:.2%}*")

        # Создание графика
        plt.figure(figsize=[18, 14])
        plt.suptitle('Ключевые метрики за неделю', fontsize=19, fontweight='bold')
        
        plt.subplot(2, 2, 1)
        plt.plot(df_metrics['day'], df_metrics['DAU'], label='DAU', color='tab:gray', marker='o', linewidth=3)
        plt.title('DAU', fontsize=15, fontweight='bold')
        plt.grid()

        plt.subplot(2, 2, 2)
        plt.plot(df_metrics['day'], df_metrics['views'], label='Просмотры', color='tab:gray', marker='o', linewidth=3)
        plt.title('Просмотры', fontsize=15, fontweight='bold')
        plt.grid()

        plt.subplot(2, 2, 3)
        plt.plot(df_metrics['day'], df_metrics['likes'], label='Лайки', color='tab:gray', marker='o', linewidth=3)
        plt.title('Лайки', fontsize=15, fontweight='bold')
        plt.grid()

        plt.subplot(2, 2, 4)
        plt.plot(df_metrics['day'], df_metrics['CTR'], label='CTR', color='tab:gray', marker='o', linewidth=3)
        plt.title('CTR', fontsize=15, fontweight='bold')
        plt.grid()

        # Сохранение графика в объект-изображение
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'test_plot.png'
        plt.close()

        return msg, plot_object

    @task()
    def send_report(report_data):
        """
        Отправляеи отчёт и график в Telegram.
        """
        msg, plot_object = report_data
        bot = telegram.Bot(token=my_token)

        bot.sendMessage(chat_id=chat_id, text=msg, parse_mode="Markdown")
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)

    # определение последовательности задач
    df_metrics = extract_data()
    report_data = transform_and_plot(df_metrics)
    send_report(report_data)

n_galina_report = n_galina_report()
