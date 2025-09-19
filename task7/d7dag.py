from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
import pendulum
import requests

default_args = {
    'owner': 'AGanshin',
    'depends_on_past': False,
    'start_date': pendulum.datetime(year=2022, month=6, day=1).in_timezone('Europe/Moscow'),
    'email': ['dom@dom.ru'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}
# DAG3
from airflow.operators.python import PythonOperator
import pandas as pd
from sqlalchemy import inspect, create_engine
from dateutil.relativedelta import relativedelta
from datetime import datetime
from pandas.io import sql
import time

# pip install openpyxl
dag3 = DAG('AGanshin003',
           default_args=default_args,
           description="seminar_7",
           catchup=False,
           schedule_interval='0 8 * * *')


def hello(**kwargs):
    encoding = "ISO-8859-1"

    df = pd.read_excel('/home/dom/d4_1.xlsx')
    con = create_engine("mysql://Airflow:1@localhost:33061/spark")
    print(df)
    df['долг'] = df['Платеж по основному долгу'].cumsum().round(2)
    df['проценты'] = df['Платеж по процентам'].cumsum().round(2)
    df.to_sql('credit', con, schema='spark', if_exists='replace', index=False)

    df1 = pd.read_excel('/home/dom/d4_2.xlsx')
    df1['долг'] = df1['Платеж по основному долгу'].cumsum().round(2)
    df1['проценты'] = df1['Платеж по процентам'].cumsum().round(2)
    df1.to_sql('credit', con, schema='spark', if_exists='append', index=False)

    df2 = pd.read_excel('/home/dom/d4_3.xlsx')
    df2['долг'] = df2['Платеж по основному долгу'].cumsum().round(2)
    df2['проценты'] = df2['Платеж по процентам'].cumsum().round(2)
    df2.to_sql('credit', con, schema='spark', if_exists='append', index=False)


t2 = PythonOperator(
    task_id='python3',
    dag=dag3,
    python_callable=hello,
    op_kwargs={'my_keyword': 'Airflow 1234'}
)