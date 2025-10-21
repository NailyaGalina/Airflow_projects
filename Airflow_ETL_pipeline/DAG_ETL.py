from datetime import datetime, timedelta
import pandas as pd
import pandahouse as ph
from airflow.decorators import dag, task

# Параметры подключения к БД
connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'user': 'student',
    'password': '****',
    'database': 'simulator_20250820'
}


connection_test = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'user': 'student-rw',
    'password': '****',
    'database': 'test'
}

# Дефолтные параметры
default_args = {
    'owner': 'n-galina',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 9, 16),
}

# Интервал запуска DAG
schedule_interval = '0 14 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def n_galina():
   
    @task()
    def extract_feed():
        query = """
        SELECT 
            user_id,
            os,
            gender,
            age,
            countIf(action = 'view') as views,
            countIf(action = 'like') as likes,
            toDate(time) as event_date
        FROM simulator_20250820.feed_actions
        WHERE toDate(time) = yesterday()
        GROUP BY user_id, os, gender, age, event_date
        """
        feed = ph.read_clickhouse(query, connection=connection)
        return feed
  
    @task()
    def messages_sent():
        query = """
        SELECT 
            user_id,
            os,
            gender,
            age,
            count() as messages_sent,
            count(DISTINCT receiver_id) as users_sent,
            toDate(time) as event_date
        FROM simulator_20250820.message_actions
        WHERE toDate(time) = yesterday()
        GROUP BY user_id, os, gender, age, event_date
        """
        messages_sent_1 = ph.read_clickhouse(query, connection=connection)
        return messages_sent_1
 
    @task()
    def messages_received():
        query = """
        SELECT 
            receiver_id as user_id,
            os,
            gender,
            age,
            count() as messages_received,
            count(DISTINCT user_id) as users_received,
            toDate(time) as event_date
        FROM simulator_20250820.message_actions
        WHERE toDate(time) = yesterday()
        GROUP BY receiver_id, os, gender, age, event_date
        """
        messages_received_1 = ph.read_clickhouse(query, connection=connection)
        return messages_received_1
  
    # Объединяем таблицы
    @task()
    def merge_data(feed, messages_sent_1, messages_received_1):
     
        merged = feed.merge(
            messages_sent_1, 
            on=['user_id', 'os', 'gender', 'age', 'event_date'], 
            how='outer'
        ).fillna(0)

        merged_1 = merged.merge(
            messages_received_1, 
            on=['user_id', 'os', 'gender', 'age', 'event_date'], 
            how='outer'
        ).fillna(0)
        return merged_1

    # Создаем срез по операционной системе
    @task()
    def transform_os(merged_1):
        os_metrics = merged_1[['event_date', 'os', 'views', 'likes', 'messages_received',
                           'messages_sent', 'users_received', 'users_sent']].groupby(
            ['event_date', 'os'], as_index=False).sum().rename(columns={'os': 'dimension_value'})
        os_metrics.insert(1, 'dimension', 'os')
        return os_metrics

    # Создаем срез по полу
    @task()
    def transform_gender(merged_1):
        gender_metrics = merged_1[['event_date', 'gender', 'views', 'likes', 'messages_received',
                               'messages_sent', 'users_received', 'users_sent']].groupby(
            ['event_date', 'gender'], as_index=False).sum().rename(columns={'gender': 'dimension_value'})
        gender_metrics.insert(1, 'dimension', 'gender')
        return gender_metrics

    # Создаем срез по возрасту
    @task()
    def transform_age(merged_1):
        age_metrics = merged_1[['event_date', 'age', 'views', 'likes', 'messages_received',
                            'messages_sent', 'users_received', 'users_sent']].groupby(
            ['event_date', 'age'], as_index=False).sum().rename(columns={'age': 'dimension_value'})
        age_metrics.insert(1, 'dimension', 'age')
        return age_metrics

    # Объединяем все срезы в одну таблицу
    @task()
    def load_to_clickhouse(gender_metrics, age_metrics, os_metrics):
        final_df_table = pd.concat([gender_metrics, age_metrics, os_metrics])

        final_df = final_df_table.astype({
            'views': 'int',
            'likes': 'int',
            'messages_sent': 'int',
            'messages_received': 'int',
            'users_sent': 'int',
            'users_received': 'int'
        })

        create_table_query = """
        CREATE TABLE IF NOT EXISTS test.nailya_galina(
            event_date Date,
            dimension String,
            dimension_value String,
            views Int64,
            likes Int64,
            messages_sent Int64,
            messages_received Int64,
            users_sent Int64,
            users_received Int64
        ) ENGINE = MergeTree()
        ORDER BY (event_date, dimension, dimension_value)
        """
        ph.execute(create_table_query, connection=connection_test)

        ph.to_clickhouse(final_df, table='nailya_galina', index=False, connection=connection_test)

    
    feed = extract_feed()
    messages_sent_1 = messages_sent()
    messages_received_1 = messages_received()

    merged_1 = merge_data(feed, messages_sent_1, messages_received_1)

    os_metrics = transform_os(merged_1)
    gender_metrics = transform_gender(merged_1)
    age_metrics = transform_age(merged_1)

    load_to_clickhouse(gender_metrics, age_metrics, os_metrics)

n_galina = n_galina()

