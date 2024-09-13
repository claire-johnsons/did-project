from airflow.decorators import dag, task, task_group
from airflow.utils.dates import days_ago
from airflow.utils import timezone
from datetime import datetime, timezone, timedelta
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

import numpy as np
import pandas as pd
import polars as pl
import os, sys, hashlib, logging

fpath = os.getenv('FUNC_PATH')
sys.path.append(fpath)

from didfunction import *

uri = os.getenv('URI')

raw = 'for_level_dictionary.xlsx'
meta = 'metadata.xlsx'
file = '{fpath}{name}'

@dag(dag_id='one_time',
     start_date=days_ago(1),
     tags=['did','test'],
     schedule=None)
def onetime():

    @task(task_id='create_df')
    def createdf(file):

        df = create_df(file.format(fpath=fpath, name=raw))

        return df
    
    @task(task_id='create_meta')
    def createmeta(df):

        return create_meta(df)
    
    @task(task_id='write_meta')
    def write_meta(df, uri):

        pldf = pl.from_pandas(df)
        pldf.write_database(table_name='metadata',
                            connection=uri,
                            if_table_exists='replace')
        
    @task(task_id='split_number')
    def split_num(df):

        col_name = df.iloc[:1,:8].columns.tolist()

        df = df.groupby(by=col_name).sum().reset_index()

        df = df.set_index(['hash_app','รหัสแอพ'])
        df = df.select_dtypes(include='number')
        df = pd.DataFrame(df.stack())
        df.index.names = ['hash_app','app_code','month']
        df = df.rename(columns={0:'number'})

        zero = df[df['number']==0].index
        df = df.drop(index=zero)
        df = df.reset_index()
        # df['month'] = pd.to_datetime(df['month'], format='%m/%Y')
        # df = df.sort_index(level=0)

        return df
        
    @task(task_id='write_number')
    def write_num(df, uri):
        pldf = pl.from_pandas(df)
        pldf.write_database(table_name='number',
                            connection=uri,
                            if_table_exists='replace')



    df = createdf(file.format(fpath=fpath, name=raw))

    meta = createmeta(df)
    num = split_num(df)

    df >> [meta,num]
    meta >> write_meta(meta, uri)
    num >> write_num(num, uri)
    
onetime()
