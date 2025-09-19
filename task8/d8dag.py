import datetime
import os
import requests
import pendulum
import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.models import Connection
from airflow import settings
from sqlalchemy import inspect, create_engine

os.environ["no_proxy"] = "*"


@dag(
    dag_id="wether-tlegram",
    schedule="@once",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def WetherETL():
    send_message_telegram_task = TelegramOperator(
        task_id='send_message_telegram',
        telegram_conn_id='telegram_conn',
        token='7248934509:AAE4KtbM5wDjmrU-xFH8bMAwBwDZw-C_cMA',
        chat_id='839424934',
        text='Wether in Moscow \nYandex: ' + "{{ ti.xcom_pull(task_ids=['yandex_wether'],key='wether')[0]}}" + " degrees" +
             "\nOpen wether: " + "{{ ti.xcom_pull(task_ids=['open_wether'],key='open_wether')[0]}}" + " degrees",
    )

    @task(task_id='yandex_wether')
    def get_yandex_wether(**kwargs):
        ti = kwargs['ti']
        url = "https://api.weather.yandex.ru/v2/informers/?lat=55.75396&lon=37.620393"

        payload = {}
        headers = {
            'X-Yandex-API-Key': '33f45b91-bcd4-46e4-adc2-33cfdbbdd88e'
        }
        response = requests.request("GET", url, headers=headers, data=payload)
        print("test")
        a = response.json()['fact']['temp']
        print(a)
        ti.xcom_push(key='wether', value=response.json()['fact']['temp'])

    #        return str(a)
    @task(task_id='open_wether')
    def get_open_wether(**kwargs):
        ti = kwargs['ti']
        url = "https://api.openweathermap.org/data/2.5/weather?lat=55.749013596652574&lon=37.61622153253021&appid=2cd78e55c423fc81cebc1487134a6300"

        payload = {}
        headers = {}

        response = requests.request("GET", url, headers=headers, data=payload)
        print("test")
        a = round(float(response.json()['main']['temp']) - 273.15, 2)
        print(a)
        ti.xcom_push(key='open_wether', value=round(float(response.json()['main']['temp']) - 273.15, 2))

    #        return str(a)
    @task(task_id='python_wether')
    def get_wether(**kwargs):
        print("Yandex " + str(kwargs['ti'].xcom_pull(task_ids=['yandex_wether'], key='wether')[0]) + " Open " + str(
            kwargs['ti'].xcom_pull(task_ids=['open_wether'], key='open_wether')[0]))

    @task(task_id='python_table')
    def get_wether_table(**kwargs):
        con = create_engine("mysql://Airflow:1@localhost:33061/spark")
        data = [[str(kwargs['ti'].xcom_pull(task_ids=['yandex_wether'], key='wether')[0]),
                 str(kwargs['ti'].xcom_pull(task_ids=['open_wether'], key='open_wether')[0]), datetime.datetime.now()]]
        df = pd.DataFrame(data)
        df.to_sql('wether', con, schema='spark', if_exists='replace', index=False)

    get_yandex_wether() >> get_open_wether() >> get_wether() >> get_wether_table() >> send_message_telegram_task


dag = WetherETL()