from airflow.decorators import dag, task, task_group
from airflow.utils.dates import days_ago
from airflow.utils import timezone
from datetime import datetime, timezone, timedelta
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
import sys, os

# import psycopg2
from dotenv import load_dotenv

load_dotenv()
fpath = os.getenv('FUNC_PATH')
email = os.getenv('EMAIL')

sys.path.append(fpath)

from didfunction import *


default_args = {
    'owner': 'airflow',
    # 'depends_on_past': True,
    # 'email': 'airflow@example.com',
    # 'email_on_failure': True,
    # 'email_on_retry': False,
    # 'retries': 3,
    # 'retry_delay': timedelta(minutes=5),
    # 'retry_exponential_backoff': False,
    # 'max_retry_delay': timedelta(hours=1),
    # 'start_date': days_ago(1),
    # 'end_date': datetime(2024, 12, 31),
    'schedule_interval': None,
    'tags': ['did','test']
    # 'catchup': False,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(minutes=30),
    # 'queue': 'default',
    # 'priority_weight': 1,
    # 'wait_for_downstream': True,
    # 'trigger_rule': 'all_success',
    # 'pool': 'default_pool'
}
file = 'for_level_dictionary.xlsx'

# bangkok_zone = datetime.now(tz=timezone(timedelta(hours=7)))

@dag(dag_id='did-ovv-test',
    # tags=['did','test'],
    start_date=days_ago(1),
    # schedule=None
    default_args=default_args
    )
def did_dag():

    # emp = EmptyOperator(task_id='emp1')
    @task(task_id='create-df')
    def emp(file):
        df = create_df(file)

        return df.iloc[2,3]
    
    df = PythonOperator(task_id='df',
                        python_callable=create_df,
                        op_kwargs={'file':f'{os.getcwd()}/external/{file}'})
    @task
    def get():
        return os.getcwd()
    
    # emp(f'{os.getcwd()}/external/{file}') #>> create_df("for_level_dictionary.xlsx")
    # get()
    df
did_dag()









