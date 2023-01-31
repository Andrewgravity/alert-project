from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.sensors.filesystem import FileSensor
from smart_file_sensor import SmartFileSensor
from airflow.utils.email import send_email_smtp
from airflow.utils.db import provide_session
from airflow.models import XCom

import os
from datetime import timedelta
from airflow.utils.dates import days_ago
from datetime import datetime
import sys
import pandas as pd
import csv
from json import dumps
import logging

dag_path = '/opt/airflow/dags' #os.getcwd()
all_files = os.listdir(dag_path)
csv_files = list(filter(lambda f: f.endswith('.csv'), all_files))

def check_for_errors_1min(ti):
    for file in csv_files:
        df = pd.read_csv(f'{dag_path}/{file}')
        new_cols = ['error_code', 'error_message', 'severity', 'log_location', 'mode', 'model', 'graphics', 'session_id', 'sdkv', 'test_mode', 'flow_id', 'flow_type', 'sdk_date', 'publisher_id', 'game_id', 'bundle_id', 'appv', 'language', 'os', 'adv_id', 'gdpr', 'ccpa', 'country_code', 'date']
        old_cols = df.columns.tolist()
        cols = dict(zip(old_cols,new_cols))
        df.rename(cols, axis=1, inplace=True)
        has_errors = df.loc[df['severity']=='Error'].shape[0] > 10
        ti.xcom_push(key='errors_1min', value = has_errors)

def check_for_errors_in_bundle_id(ti):
    for file in csv_files:
        df = pd.read_csv(f'{dag_path}/{file}')
        new_cols = ['error_code', 'error_message', 'severity', 'log_location', 'mode', 'model', 'graphics', 'session_id', 'sdkv', 'test_mode', 'flow_id', 'flow_type', 'sdk_date', 'publisher_id', 'game_id', 'bundle_id', 'appv', 'language', 'os', 'adv_id', 'gdpr', 'ccpa', 'country_code', 'date']
        old_cols = df.columns.tolist()
        cols = dict(zip(old_cols,new_cols))
        df.rename(cols, axis=1, inplace=True)
        has_errors = df.loc[df['severity']=='Error'].groupby('bundle_id').sum()[df.loc[df['severity']=='Error'].groupby('bundle_id').sum() > 10].dropna(subset=['error_code']).shape[0] > 0
        ti.xcom_push(key='errors_bundle_id', value = has_errors)


def decide_which_path_for_errors(ti):
    if ti.xcom_pull(key='errors_1min', task_ids='check_for_errors_1min') is True:
        return 'send_email'
    else:
        return 'notify'

def decide_which_path_for_bundle_id(ti):
    if ti.xcom_pull(key='errors_bundle_id', task_ids='check_for_errors_in_bundle_id') is True:
        return 'send_email'
    else:
        return 'notify'

default_args = {
    'owner':'airflow',
    'start_date': days_ago(3),
    'email_on_failure': False,
    'email': ['airflow_email@mail.ru'],
    'provide_context': True
}

dag = DAG(  
    dag_id="airflow_project",
    default_args=default_args,
    description='Data pipeline dag',
    schedule_interval=None,
    start_date=datetime.now(),
    catchup=False
)

check_one = PythonOperator(
    task_id='check_for_errors_1min',
    python_callable=check_for_errors_1min,
    dag=dag
)

check_two = PythonOperator(
    task_id='check_for_errors_in_bundle_id',
    python_callable=check_for_errors_in_bundle_id,
    dag=dag
)

branch_10_fatal_errors = BranchPythonOperator(
    task_id='branch_10_fatal_errors',
    python_callable=decide_which_path_for_errors,
    trigger_rule="all_done",
    dag=dag)

branch_10_fatal_errors_in_bundle_id = BranchPythonOperator(
    task_id='branch_10_fatal_errors_in_bundle_id',
    python_callable=decide_which_path_for_errors,
    trigger_rule="all_done",
    dag=dag)
    
sensor = SmartFileSensor(
    task_id='file_sensor',
    poke_interval=30,
    filepath=f'{dag_path}/{csv_files[0]}',
    fs_conn_id="file_system"
)

send_email = EmailOperator(
    task_id='send_email',
    to='airflow_email@mail.ru',
    subject='errors found',
    html_content="found more than 10 errors in data",
    dag=dag)

notify = BashOperator(
    task_id="notify_if_no_errors",
    bash_command='echo "no errors found"',
    dag=dag,
)

sensor >> check_one >>  branch_10_fatal_errors >> [send_email, notify]
sensor >> check_two >> branch_10_fatal_errors_in_bundle_id >> [send_email, notify]