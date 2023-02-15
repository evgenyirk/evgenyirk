import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
import requests
import pandahouse

from airflow.decorators import dag, task


default_args = {
    'owner': 'e.artjuhov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 13)
    }
schedule_interval = '0 11 * * *'

connection = {
    'host':'https://clickhouse.lab.karpov.courses',
    'password': 'dpo_python_2020',
    'user':'student',
    'database': 'simulator_20221120'
    }

my_token = '5289111807:AAFOzNB3KxTGZDr9w1oCeTJ8m15SgFNlD9M'
chat_id = -817095409
bot = telegram.Bot(token=my_token)

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_artjuhov_report_feed():

    @task
    def extract_feed():
        query ="""SELECT 
                   count(DISTINCT user_id) AS DAU,
                   countIf(user_id,action='view') as views,
                   countIf(user_id, action='like') as likes,
                   toDate(time) event_date
               FROM 
                   simulator_20221120.feed_actions
               WHERE 
                   toDate(time) BETWEEN today() - 7 AND yesterday()
               GROUP BY  event_date
               ORDER BY event_date"""

        df_cube = pandahouse.read_clickhouse(query,connection=connection)
        return df_cube
       
    @task
    def transform(df_cube):
        df_cube['CTR'] = df_cube['likes']/df_cube['views']
        df_cube['DAU'] = df_cube['DAU'] / 1000
        df_cube['likes'] = df_cube['likes'] / 1000
        df_cube['views'] = df_cube['views'] / 1000
        df_cube['short_date'] = df_cube.event_date.apply(lambda x: x.strftime('%d-%m'))
        df = df_cube
        return df
    
      
    @task
    def send_message(df):
        a = " "
        message  = f"""Лента новостей. Метрики за вчера, {df.event_date[6].strftime('%d/%m/%Y')}\n\n\
DAU{a*20}{round(df.DAU[6],1)}k\nПросмотры{a*5}{round(df.views[6])}k\n\
Лайки{a*16}{round(df.likes[6])}k\nCTR{a*20}{round(df.CTR[6],3)}"""
        bot.sendMessage(chat_id=chat_id,text=message)
        
    @task
    def send_charts(df):
        ax = sns.lineplot(df.event_date,df.DAU)
        ax.set_xticklabels(df.short_date)
        plt.title('DAU')
        plt.xlabel('Дата')
        plt.ylabel('DAU, тысяч')
        sns.despine(top = True, right = True)


        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'test_plot.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        
        ax = sns.lineplot(df.event_date,df.views,color='green')
        ax.set_xticklabels(df.short_date)
        plt.title('Просмотры')
        plt.xlabel('Дата')
        plt.ylabel('Просмотры, тысяч')
        sns.despine(top = True, right = True)

        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'test_plot.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        
        ax = sns.lineplot(df.event_date,df.likes,color='red')
        ax.set_xticklabels(df.short_date)
        plt.title('Лайки')
        plt.xlabel('Дата')
        plt.ylabel('Лайки, тысяч')
        sns.despine(top = True, right = True)

        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'test_plot.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        
        ax = sns.lineplot(df.event_date,df.CTR,color='orange')
        ax.set_xticklabels(df.short_date)
        plt.title('CTR')
        plt.xlabel('Дата')
        sns.despine(top = True, right = True)

        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'test_plot.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        
    
    df_cube = extract_feed()
    df = transform(df_cube)
    send_message(df)
    send_charts(df)
    
dag_artjuhov_report_feed = dag_artjuhov_report_feed()
