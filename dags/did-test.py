from airflow.decorators import dag, task, task_group
from airflow.utils.dates import days_ago
from airflow.utils import timezone
from datetime import datetime, timezone, timedelta
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
import sys, os

# import psycopg2
from dotenv import load_dotenv
import pandas as pd
import numpy as np
import polars as pl
import psycopg2

load_dotenv()
fpath = os.getenv('FUNC_PATH')
email = os.getenv('EMAIL')
uri = os.getenv('URI')

sys.path.append(fpath)

from didfunction import *


default_args = {
    'owner': 'airflow',
    # 'depends_on_past': True,
    # 'email': 'airflow@example.com',
    # 'email_on_failure': True,
    # 'email_on_retry': False,
    # 'retries': 0,
    # 'retry_delay': timedelta(minutes=5),
    # 'retry_exponential_backoff': False,
    # 'max_retry_delay': timedelta(hours=1),
    'start_date': days_ago(1),
    # 'end_date': datetime(2024, 12, 31),
    'schedule_interval': None,
    # 'tags': ['did','test']
    # 'catchup': False,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(minutes=30),
    # 'queue': 'default',
    # 'priority_weight': 1,
    # 'wait_for_downstream': True,
    # 'trigger_rule': 'all_success',
    # 'pool': 'default_pool'
}
raw = 'app_25042567-16092567.xlsx'
# meta = 'metadata.xlsx'
file = '{fpath}/{name}'

# bangkok_zone = datetime.now(tz=timezone(timedelta(hours=7)))

@dag(dag_id='did-ovv-test',
    tags=['did','test'],
    # start_date=days_ago(1),
    # schedule=None
    default_args=default_args
    )
def did_dag():

    @task(task_id='create-new-df')
    def create_df_task(file):
        # cleaning processing included
        return create_df(file)

    @task(task_id='read-previous-meta', trigger_rule='all_success')
    def read_pre_meta(uri):

        premeta = pl.read_database_uri(query='select * from metadata',uri=uri)

        return premeta.to_pandas()
    
    @task(task_id='create-current-meta', trigger_rule='all_success')
    def current_meta(df):
        # no check condition
        return create_meta(df)
    
    @task(task_id='check-new-app', trigger_rule='all_success')
    def task_check_new_app(pre,cur):
        # columns mismatch checking included
        return check_new_app(pre, cur)

    
    @task(task_id='write-meta', trigger_rule='all_success')
    def write_meta(df):
        pldf = pl.from_pandas(df)
        pldf.write_database(table_name='metadata', connection=uri, if_table_exists='append')

# --------------------------------------

    @task(task_id='read-previous-num')
    def read_pre_num(uri):

        prenum = pl.read_database_uri(query='select * from number_of_users',uri=uri)

        return prenum.to_pandas()
    
    @task(task_id='get-new-month-name', trigger_rule='all_success')
    def task_detect_new_month(old, df):
        # return list of all new month columns added [0]
        # return df from df task [1]
        return detect_new_month(old, df)


    @task(task_id='create-current-number', trigger_rule='all_success')
    def task_split_new(df, new_month):
        # no check condition
        # drop row amount is zero
        return split_new(df, new_month)
    

    '''   
    @task(task_id='group-by-sum', trigger_rule='all_success')
    def task_group_by_sum(pre_num, cur_num):
        # concat new month to previous df
        return group_by_sum(pre_num, cur_num)
    '''
    
    @task(task_id='write-number', trigger_rule='all_success')
    def write_number(df):

        pldf = pl.from_pandas(df)
        pldf.write_database(table_name='number', connection=uri, if_table_exists='append')
    

    # @task(task_id='write-metadata')
    # def wr_meta(df):
    #     df.to_excel('external/newapp.xlsx',index=False)

    # @task(task_id='write-number')
    # def wr_num(df):
    #     df.to_excel('external/newnum.xlsx',index=False)

    df = create_df_task(file.format(fpath=fpath, name=raw))
    pre_meta = read_pre_meta(uri)
    cur_meta = current_meta(df)
    all_new = task_check_new_app(pre_meta, cur_meta)
    w_meta = write_meta(all_new)

    # ---------------------------
    pre_num = read_pre_num(uri)
    new_month = task_detect_new_month(pre_num, df)
    cur_num = task_split_new(df, new_month)
    # group = task_group_by_sum(pre_num, cur_num)
    w_num = write_number(cur_num)

    [pre_meta, df] >> cur_meta >> all_new >> w_meta
    [pre_num, df] >> new_month >> cur_num >> w_num

did_dag()