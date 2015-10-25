from __future__ import print_function
from builtins import range
from airflow.operators import PythonOperator, EmailOperator
from airflow.models import DAG
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import time

from pymongo import MongoClient
from bson.son import SON

seven_days_ago = datetime.combine(datetime.today() - timedelta(7),
                                  datetime.min.time())

args = {
    'owner': 'airflow',
    'start_date': seven_days_ago,
}

params = {
    'foo': 'foo',
}

dag = DAG(dag_id='example_pymongo_and_aggregate_operator', default_args=args, params=params)


def connect_to_mongodb_and_aggregate_day(ds, **kwargs):
    db = MongoClient().test
    tmp_created_collection_per_day_name = 'page_per_day_hits_tmp';
    pipeline = [{"$project":{'page': '$PAGE', 'time': { 'y': {'$year':'$DATE'} , 'm':{'$month':'$DATE'}, 'day':{'$dayOfMonth':'$DATE'}}}}, {'$group':{'_id':{'p':'$page','y':'$time.y','m':'$time.m','d':'$time.day'}, 'daily':{'$sum':1}}},{'$out': tmp_created_collection_per_day_name}]
    results = db.logs.aggregate(pipeline)
    print("Aggregated daily metrics")
    return 'Whatever you return gets printed in the logs'


def connect_to_mongodb_and_aggregate_hour(ds, **kwargs):
    db = MongoClient().test
    tmp_created_collection_per_hour_name = 'page_per_hour_hits_tmp';
    pipeline = [{"$project":{'page': '$PAGE', 'time': { 'y': {'$year':'$DATE'} , 'm':{'$month':'$DATE'}, 'day':{'$dayOfMonth':'$DATE'}, 'h':{'$hour':'$DATE'}}}}, {'$group':{'_id':{'p':'$page','y':'$time.y','m':'$time.m','d':'$time.day', 'h':'$time.h'}, 'hourly':{'$sum':1}}},{'$out': tmp_created_collection_per_hour_name}]
    results = db.logs.aggregate(pipeline)
    print("Aggregated hour metrics")
    return 'Whatever you return gets printed in the logs'

run_this = PythonOperator(
    task_id='connect_to_mongodb_and_aggregate_day',
    provide_context=True,
    python_callable=connect_to_mongodb_and_aggregate_day,
    dag=dag)

run_this_also = PythonOperator(
    task_id='connect_to_mongodb_and_aggregate_hour',
    provide_context=True,
    python_callable=connect_to_mongodb_and_aggregate_hour,
    dag=dag)

run_this_also.set_upstream(run_this)

send_email_notification_flow_successful = EmailOperator(
    task_id='send_email_notification_flow_successful',
    to="nowhere@nowhere.com",
    subject='custom email from airflow',
    html_content="{{ params['foo'](execution_date) }}",
    params=params,
    dag=dag)

send_email_notification_flow_successful.set_upstream(run_this_also)