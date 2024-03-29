""" 
    Для запуска возможно прийдется изменить переменные:
    DATA_FOLDER, SQL_FOLDER которые хранят пути к sql скриптам
    и папке, куда выгружаем файлы s3.
    Перед запуском DAG необходимо добавить поля status в таблицы и
    создать таблицу для витрины, файл: alter_schema.sql
"""

import time
import requests
import json
import pandas as pd
import os

from jinja2 import Template
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.http_hook import HttpHook

# параметры
HTTP_CONN_ID = HttpHook.get_connection('http_conn_id')
API_KEY = HTTP_CONN_ID.extra_dejson.get('api_key')
BASE_URL = HTTP_CONN_ID.host

POSTGRES_CONN_ID = 'postgresql_de'

NICKNAME = 'feyginalex'
COHORT = '12'

DATA_FOLDER = '/lessons/dags/src/dags/data'
SQL_FOLDER  = '/lessons/dags/migrations'

HEADERS = {
    'X-Nickname': NICKNAME,
    'X-Cohort': COHORT,
    'X-Project': 'True',
    'X-API-KEY': API_KEY,
    'Content-Type': 'application/x-www-form-urlencoded'
}

ARGS = {
    "owner": "student",
    'email': ['student@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2
}

BUSINESS_DT = '{{ ds }}'

def generate_report(ti):
    print('Making request generate_report')

    response = requests.post(f'{BASE_URL}/generate_report', headers=HEADERS)
    response.raise_for_status()
    task_id = json.loads(response.content)['task_id']
    ti.xcom_push(key='task_id', value=task_id)
    print(f'Response is {response.content}')


def get_report(ti):
    print('Making request get_report')
    task_id = ti.xcom_pull(key='task_id')

    report_id = None

    for i in range(20):
        response = requests.get(f'{BASE_URL}/get_report?task_id={task_id}', headers=HEADERS)
        response.raise_for_status()
        print(f'Response is {response.content}')
        status = json.loads(response.content)['status']
        if status == 'SUCCESS':
            report_id = json.loads(response.content)['data']['report_id']
            break
        else:
            time.sleep(10)

    if not report_id:
        raise TimeoutError()

    ti.xcom_push(key='report_id', value=report_id)
    print(f'Report_id={report_id}')


def get_increment(date, ti):
    print('Making request get_increment')
    report_id = ti.xcom_pull(key='report_id')
    response = requests.get(
        f'{BASE_URL}/get_increment?report_id={report_id}&date={str(date)}T00:00:00',
        headers=HEADERS)
    response.raise_for_status()
    print(f'Response is {response.content}')

    increment_id = json.loads(response.content)['data']['increment_id']
    if not increment_id:
        raise ValueError(f'Increment is empty. Most probably due to error in API call.')
    
    ti.xcom_push(key='increment_id', value=increment_id)
    print(f'increment_id={increment_id}')

# вынес загрузку файла в отдельный шаг
def load_data_from_s3(filename, date, ti):
    increment_id = ti.xcom_pull(key='increment_id')
    s3_filename = f'https://storage.yandexcloud.net/s3-sprint3/cohort_{COHORT}/{NICKNAME}/project/{increment_id}/{filename}'
    print(s3_filename)
    local_filename = date.replace('-', '') + '_' + filename
    local_full_path = f"{DATA_FOLDER}/{local_filename}"
    print(local_full_path)
    response = requests.get(s3_filename)
    response.raise_for_status()
    open(local_full_path, "wb").write(response.content)
    print(response.content)
    ti.xcom_push(key='file_path', value=local_full_path)

def upload_data_to_staging(date, pg_table, pg_schema, ti):
    filename = ti.xcom_pull(key='file_path')
    df = pd.read_csv(filename, index_col=0)
    df=df.drop_duplicates(subset=['uniq_id'])

    if 'status' not in df.columns:
        df['status'] = 'shipped'

    postgres_hook = PostgresHook(POSTGRES_CONN_ID)

    with open(f'{SQL_FOLDER}/staging.clear_current_data.sql') as f: # читаем содержимое шаблона
        sql_template = f.read()
    
    sql = Template(sql_template).render(ds=date) # рендерим jinja template  
    postgres_hook.run(sql = sql) # удаляем данные за текущий период в staging
    
    engine = postgres_hook.get_sqlalchemy_engine()
    row_count = df.to_sql(pg_table, engine, schema=pg_schema, if_exists='append', index=False)
    print(f'{row_count} rows was inserted')



with DAG(
        'load_sales_mart',
        default_args=ARGS,
        catchup=True,
        # start_date=datetime.today() - timedelta(days=7),
        # end_date=datetime.today() - timedelta(days=6),
        start_date=datetime.today() - timedelta(days=7),
        end_date=datetime.today() - timedelta(days=1),
        template_searchpath = SQL_FOLDER,
        max_active_runs = 1
) as dag:

    generate_report = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report)

    get_report = PythonOperator(
        task_id='get_report',
        python_callable=get_report)

    get_increment = PythonOperator(
        task_id='get_increment',
        python_callable=get_increment,
        op_kwargs={'date': BUSINESS_DT})

    download_data = PythonOperator(
        task_id='load_data_from_s3',
        python_callable=load_data_from_s3,
        op_kwargs={'date': BUSINESS_DT,
                   'filename': 'user_order_log_inc.csv'})

    load_staging = PythonOperator(
        task_id='upload_data_to_staging',   
        python_callable=upload_data_to_staging,
        op_kwargs={'date': BUSINESS_DT,
                   'pg_table': 'user_order_log', 
                   'pg_schema': 'staging'})
    
    dimension_tasks = list()
    for i in ['d_item', 'd_customer', 'd_city']:
        dimension_tasks.append(PostgresOperator(
            task_id = f'update_{i}',
            postgres_conn_id = POSTGRES_CONN_ID,
            sql = f'/migrations/mart.{i}.sql',
            parameters={"date": {BUSINESS_DT}} 
            )
        )

    update_f_sales = PostgresOperator(
        task_id='update_f_sales',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql= f'/migrations/mart.f_sales.sql',
        parameters={"date": {BUSINESS_DT}} 
    )

    update_f_customer_retention = PostgresOperator(
        task_id='update_f_customer_retention',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql= f'/migrations/mart.f_customer_retention_update.sql',
        parameters={"date": {BUSINESS_DT}} 
    )

    (       
        generate_report
        >> get_report
        >> get_increment
        >> download_data
        >> load_staging
        >> dimension_tasks
        >> update_f_sales
        >> update_f_customer_retention
    )
