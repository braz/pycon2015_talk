from __future__ import print_function
from builtins import range
from airflow.operators import PythonOperator, EmailOperator
from airflow.models import DAG
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import time
from monary import Monary

seven_days_ago = datetime.combine(datetime.today() - timedelta(7),
                                  datetime.min.time())
default_args = {
    'owner': 'airflow',
    'start_date': seven_days_ago,
}

params = {
    'foo': 'foo',
}

dag = DAG(dag_id='connect_to_monary_and_email_operator', default_args=default_args, params=params)

def connect_to_monary_and_email_operator(ds, **kwargs):
    m = Monary()
    pipeline = [{"$group": {"_id": "$state", "totPop": {"$sum": "$pop"}}}]
    states, population = m.aggregate("zips", "data", pipeline, ["_id", "totPop"], ["string:2", "int64"])
    strs = list(map(lambda x: x.decode("utf-8"), states))
    result = list("%s: %d" % (state, pop) for (state, pop) in zip(strs, population))
    print (result)

run_this = PythonOperator(
    task_id='connect_to_monary_and_email_operator',
    provide_context=True,
    python_callable=connect_to_monary_and_email_operator,
    dag=dag)

send_email_notification_flow_successful = EmailOperator(
    task_id='send_email_notification_flow_successful',
    to="nowhere@nowhere.com",
    subject='custom email from airflow',
    html_content="{{ params['foo'](execution_date) }}",
    params=params,
    dag=dag)

send_email_notification_flow_successful.set_upstream(run_this)