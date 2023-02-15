import telegram
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
from datetime import date, timedelta, datetime
import requests
import pandahouse

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

default_args = {
    'owner': 'e.artjuhov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 12, 18)
}

schedule_interval = '0 11 * * *'

connection = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'simulator_20221120',
                      'user':'student',
                      'password':'dpo_python_2020'
                     }


my_token = '5289111807:AAFOzNB3KxTGZDr9w1oCeTJ8m15SgFNlD9M'
my_bot = telegram.Bot(token=my_token)
chat_id = -817095409

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def artjuhov_l7_t2 ():

    @task()
    def report_application():

        q_feed_msg_dau = """
                SELECT toStartOfDay(toDateTime(date_dt)) AS day,
                       count(DISTINCT fa_user_id) AS "DAU feed + message"
                  FROM
                       (SELECT toDate(fa.time) date_dt,
                               fa.user_id AS fa_user_id,
                               ma.user_id AS ma_user_id
                          FROM simulator_20221120.feed_actions fa
                         INNER JOIN simulator_20221120.message_actions ma ON fa.user_id = ma.user_id
                           AND toDate(fa.time) = toDate(ma.time)
                         ORDER BY toDate(fa.time) DESC) AS virtual_table
                         GROUP BY toStartOfDay(toDateTime(date_dt))
                         ORDER BY toStartOfDay(toDateTime(date_dt)) DESC
                  """
        feed_msg_dau = pandahouse.read_clickhouse(q_feed_msg_dau, connection=connection)

        q_only_feed = """
                    SELECT toStartOfDay(toDateTime(date_dt)) AS day,
                           count(DISTINCT fa_user_id) AS "DAU only feed"
                      FROM
                           (SELECT toDate(fa.time) date_dt,
                                   fa.user_id AS fa_user_id,
                                   ma.user_id AS ma_user_id
                              FROM simulator_20221120.feed_actions fa
                              LEFT JOIN simulator_20221120.message_actions ma ON fa.user_id = ma.user_id
                               AND toDate(fa.time) = toDate(ma.time)
                             WHERE ma.user_id = 0
                             ORDER BY toDate(fa.time) DESC) AS virtual_table
                             GROUP BY toStartOfDay(toDateTime(date_dt))
                             ORDER BY toStartOfDay(toDateTime(date_dt)) DESC
                      """
        only_feed = pandahouse.read_clickhouse(q_only_feed, connection=connection)

        q_only_msg = """
                    SELECT toStartOfDay(toDateTime(date_dt)) day,
                           count(DISTINCT ma_user_id) AS "DAU only message"
                      FROM
                           (SELECT toDate(ma.time) date_dt,
                                   ma.user_id AS ma_user_id,
                                   fa.user_id AS fa_user_id
                              FROM simulator_20221120.message_actions ma
                              LEFT JOIN simulator_20221120.feed_actions fa ON fa.user_id = ma.user_id
                               AND toDate(fa.time) = toDate(ma.time)
                             WHERE fa.user_id = 0
                             ORDER BY toDate(fa.time) DESC) AS virtual_table
                             GROUP BY toStartOfDay(toDateTime(date_dt))
                             ORDER BY toStartOfDay(toDateTime(date_dt)) DESC
                      """
        only_msg = pandahouse.read_clickhouse(q_only_msg, connection=connection)
        
        
        q_msg_per_user = """
                         SELECT toStartOfDay(toDateTime(time)) AS day,
                                count(user_id) / count(DISTINCT user_id) AS "Сообщений на пользователя"
                           FROM simulator_20221120.message_actions
                          GROUP BY toStartOfDay(toDateTime(time))
                          ORDER BY toStartOfDay(toDateTime(time)) DESC
                          """
        msg_per_user = pandahouse.read_clickhouse(q_msg_per_user, connection=connection)
        
        q_views_per_user = """
                         SELECT toStartOfDay(toDateTime(time)) as day,
                                countIf(user_id, action='view') / count(DISTINCT user_id) AS "Просмотров на пользователя"
                           FROM simulator_20221120.feed_actions
                          GROUP BY toStartOfDay(toDateTime(time))
                          ORDER BY toStartOfDay(toDateTime(time)) DESC;
                          """
       
        views_per_user = pandahouse.read_clickhouse(q_views_per_user, connection=connection)
        
        q_likes_per_user = """
                         SELECT toStartOfDay(toDateTime(time)) as day,
                                countIf(user_id, action='like') / count(DISTINCT user_id) AS "Лайков на пользователя"
                           FROM simulator_20221120.feed_actions
                          GROUP BY toStartOfDay(toDateTime(time))
                          ORDER BY toStartOfDay(toDateTime(time)) DESC;
                          """
       
        likes_per_user = pandahouse.read_clickhouse(q_likes_per_user, connection=connection)
        
        
        q_msg_count = """
                      SELECT toISOWeek(time) AS week,
                             count(user_id) AS "Кол-во Сообщений"
                        FROM simulator_20221120.message_actions
                        GROUP BY toISOWeek(time)
                        ORDER BY toISOWeek(time) DESC;
                        """
        msg_count = pandahouse.read_clickhouse(q_msg_count, connection=connection)
        
        
        q_stickiness = """
                           
                     SELECT toStartOfDay(toDateTime(date_dt)) AS day,
                           sum("Stickiness") AS "Stickiness"
                    FROM
                      (WITH mau_t AS
                         (SELECT count(DISTINCT user_id) AS mau
                          FROM simulator_20221120.feed_actions
                          WHERE time BETWEEN time - INTERVAL 30 DAY AND toDate(now())),
                            dau_t AS
                         (SELECT toDate(time) AS date_dt,
                                 COUNT(DISTINCT user_id) AS dau
                          FROM simulator_20221120.feed_actions
                          GROUP BY toDate(time)) SELECT date_dt,
                                                        round(dau /
                                                                (SELECT mau
                                                                 FROM mau_t), 2) AS Stickiness
                       FROM dau_t
                       ORDER BY date_dt) AS virtual_table
                    GROUP BY toStartOfDay(toDateTime(date_dt))
                    ORDER BY toStartOfDay(toDateTime(date_dt)) DESC;
        
        """
        
        stickiness = pandahouse.read_clickhouse(q_stickiness, connection=connection)


        ### сообщение для отправки в телеграмм
        msg = "Репорт по работе приложения на " + str(date.today()) + ':' + '\n'\
              + 'УНИКАЛЬНЫЕ ПОЛЬЗОВАТЕЛИ ЛЕНТЫ И СООБЩЕНИЙ (одновременно):' + '\n'\
              + 'За вчера = ' + str(feed_msg_dau.iloc[1, 1]) + '\n' \
              + 'Неделю назад = ' + str(feed_msg_dau.iloc[7, 1]) + '\n' \
              + 'Месяц назад = ' + str(feed_msg_dau.iloc[30, 1]) + '\n' \
              + 'DoD = ' + str(round(feed_msg_dau.iloc[1, 1] / feed_msg_dau.iloc[2, 1], 2)) + '\n' \
              + 'WoW = ' + str(round(feed_msg_dau.iloc[1, 1] / feed_msg_dau.iloc[7, 1], 2)) + '\n' \
              + 'УНИКАЛЬНЫЕ ПОЛЬЗОВАТЕЛИ ТОЛЬКО СЕРВИСА ЛЕНТЫ:' + '\n' \
              + 'За вчера = ' + str(only_feed.iloc[1, 1]) + '\n' \
              + 'Неделю назад = ' + str(only_feed.iloc[7, 1]) + '\n' \
              + 'Месяц назад = ' + str(only_feed.iloc[30, 1]) + '\n' \
              + 'DoD = ' + str(round(only_feed.iloc[1, 1] / only_feed.iloc[2, 1], 2)) + '\n' \
              + 'WoW = ' + str(round(only_feed.iloc[1, 1] / only_feed.iloc[7, 1], 2)) + '\n' \
              + 'УНИКАЛЬНЫЕ ПОЛЬЗОВАТЕЛИ ТОЛЬКО СЕРВИСА СООБЩЕНИЙ:' + '\n' \
              + 'За вчера = ' + str(only_msg.iloc[1, 1]) + '\n' \
              + 'Неделю назад = ' + str(only_msg.iloc[7, 1]) + '\n' \
              + 'Месяц назад = ' + str(only_msg.iloc[30, 1]) + '\n' \
              + 'DoD = ' + str(round(only_msg.iloc[1, 1] / only_msg.iloc[2, 1], 2)) + '\n' \
              + 'WoW = ' + str(round(only_msg.iloc[1, 1] / only_msg.iloc[7, 1], 2)) + '\n' \
              + 'СООБЩЕНИЙ НА ПОЛЬЗОВАТЕЛЯ:' + '\n' \
              + 'За вчера = ' + str(round(msg_per_user.iloc[1, 1], 2)) + '\n' \
              + 'Неделю назад = ' + str(round(msg_per_user.iloc[7, 1], 2)) + '\n' \
              + 'Месяц назад = ' + str(round(msg_per_user.iloc[30, 1], 2)) + '\n' \
              + 'DoD = ' + str(round(msg_per_user.iloc[1, 1] / msg_per_user.iloc[2, 1], 2)) + '\n' \
              + 'WoW = ' + str(round(msg_per_user.iloc[1, 1] / msg_per_user.iloc[7, 1], 2)) + '\n' \
              + 'ПРОСМОТРОВ НА ПОЛЬЗОВАТЕЛЯ:' + '\n' \
              + 'За вчера = ' + str(round(views_per_user.iloc[1, 1], 2)) + '\n' \
              + 'Неделю назад = ' + str(round(views_per_user.iloc[7, 1], 2)) + '\n' \
              + 'Месяц назад = ' + str(round(views_per_user.iloc[30, 1], 2)) + '\n' \
              + 'DoD = ' + str(round(views_per_user.iloc[1, 1] / views_per_user.iloc[2, 1], 2)) + '\n' \
              + 'WoW = ' + str(round(views_per_user.iloc[1, 1] / views_per_user.iloc[7, 1], 2)) + '\n' \
              + 'ЛАЙКОВ НА ПОЛЬЗОВАТЕЛЯ:' + '\n' \
              + 'За вчера = ' + str(round(likes_per_user.iloc[1, 1], 2)) + '\n' \
              + 'Неделю назад = ' + str(round(likes_per_user.iloc[7, 1], 2)) + '\n' \
              + 'Месяц назад = ' + str(round(likes_per_user.iloc[30, 1], 2)) + '\n' \
              + 'DoD = ' + str(round(likes_per_user.iloc[1, 1] / likes_per_user.iloc[2, 1], 2)) + '\n' \
              + 'WoW = ' + str(round(likes_per_user.iloc[1, 1] / likes_per_user.iloc[7, 1], 2)) + '\n' \
              + 'STICKINESS (DAU/MAU):' + '\n' \
              + 'За вчера = ' + str(round(stickiness.iloc[1, 1], 2)) + '\n' \
              + 'Неделю назад = ' + str(round(stickiness.iloc[7, 1], 2)) + '\n' \
              + 'Месяц назад = ' + str(round(stickiness.iloc[30, 1], 2)) + '\n' \
              + 'DoD = ' + str(round(stickiness.iloc[1, 1] / stickiness.iloc[2, 1], 2)) + '\n' \
              + 'WoW = ' + str(round(stickiness.iloc[1, 1] / stickiness.iloc[7, 1], 2)) + '\n' \
              + 'КОЛИЧЕСТВО СООБЩЕНИЙ:' + '\n' \
              + 'Неделю назад = ' + str(round(msg_count.iloc[1, 1], 2)) + '\n' \
              + 'Месяц назад = ' + str(round(msg_count.iloc[4, 1], 2)) + '\n' \
              + 'WoW = ' + str(round(msg_count.iloc[1, 1] / msg_count.iloc[2, 1], 2)) + '\n' \

        my_bot.sendMessage(chat_id=chat_id, text=msg)

        


        ### отправка изображения
        
        fig, axes = plt.subplots(4, 2, figsize=(14, 17))
        
        fig.suptitle("Отчет по приложению", fontsize=17, fontweight="bold")
        fig.subplots_adjust(hspace=0.69)
        fig.subplots_adjust(wspace=0.34)
        
   

        sns.lineplot(data=feed_msg_dau, x='day', y="DAU feed + message", ax=axes[0, 0], color='darkblue')
        axes[0, 0].set_title("Уникальные пользователи (Лента + Сообщения одновременно)", fontsize=10)
        axes[0, 0].tick_params(axis='x', rotation=45)
        
        sns.lineplot(data=msg_per_user, x='day', y="Сообщений на пользователя", ax=axes[1, 1], color='orangered')
        axes[1, 1].set_title("Сообщений на пользователя", fontsize=10)
        axes[1, 1].tick_params(axis='x', rotation=45)
        
        sns.lineplot(data=only_feed, x='day', y="DAU only feed", ax=axes[0, 1], color='orangered')
        axes[0, 1].set_title('Уникальные пользователи только сервиса Ленты', fontsize=10)
        axes[0, 1].tick_params(axis='x', rotation=45)
        
        sns.lineplot(data=only_msg, x='day', y="DAU only message", ax=axes[1, 0], color='g')
        axes[1, 0].set_title("Уникальные пользователи только сервиса Сообщений", fontsize=10)
        axes[1, 0].tick_params(axis='x', rotation=45)
        
        sns.lineplot(data=views_per_user, x='day', y="Просмотров на пользователя", ax=axes[2, 0], color='g')
        axes[2, 0].set_title("Просмотров на пользователя", fontsize=10)
        axes[2, 0].tick_params(axis='x', rotation=45)
        
        sns.lineplot(data=likes_per_user, x='day', y="Лайков на пользователя", ax=axes[2, 1], color='g')
        axes[2, 1].set_title("Лайков на пользователя", fontsize=10)
        axes[2, 1].tick_params(axis='x', rotation=45)
        
        sns.lineplot(data=msg_count, x='week', y="Кол-во Сообщений", ax=axes[3, 1], color='g')
        axes[3, 1].set_title("Кол-во Сообщений", fontsize=10)
        axes[3, 1].tick_params(axis='x', rotation=45)
        
        sns.lineplot(data=stickiness, x='day', y="Stickiness", ax=axes[3, 0], color='g')
        axes[3, 0].set_title("Stickiness", fontsize=10)
        axes[3, 0].tick_params(axis='x', rotation=45)

        plot_object = io.BytesIO() 
        plt.savefig(plot_object) 
        plot_object.seek(0) # передвинули курсор в начало фалового объекта
        plot_object.name = ('report_for_7_days.png')
        plt.close()

        # Отправим график
        my_bot.sendPhoto(chat_id=chat_id, photo=plot_object)


    report_application = report_application()




artjuhov_l7_t2 = artjuhov_l7_t2()
