from __future__ import print_function
from builtins import range
from airflow.operators import PythonOperator
from airflow.models import DAG
from datetime import datetime, timedelta

import time
from pymongo import MongoClient

seven_days_ago = datetime.combine(datetime.today() - timedelta(7),
                                  datetime.min.time())

args = {
    'owner': 'airflow',
    'start_date': seven_days_ago,
}

dag = DAG(dag_id='example_pymongo_operator', default_args=args)


def my_sleeping_function(random_base):
    '''This is a function that will run within the DAG execution'''
    time.sleep(random_base)


def connect_to_mongodb_and_print(ds, **kwargs):
    db = MongoClient().zips
    buildinfo = db.command("buildinfo")
    print(buildinfo)
    return 'Whatever you return gets printed in the logs'

run_this = PythonOperator(
    task_id='connect_to_mongodb_and_print',
    provide_context=True,
    python_callable=connect_to_mongodb_and_print,
    dag=dag)

for i in range(10):
    '''
    Generating 10 sleeping task, sleeping from 0 to 9 seconds
    respectively
    '''
    task = PythonOperator(
        task_id='sleep_for_'+str(i),
        python_callable=my_sleeping_function,
        op_kwargs={'random_base': i},
        dag=dag)

    task.set_upstream(run_this)
