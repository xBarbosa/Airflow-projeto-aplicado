from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import XCom
from airflow.utils.dates import days_ago
from airflow import settings

def clear_xcoms(**kwargs):
    session = settings.Session()
    session.query(XCom).delete()
    session.commit()
    session.close()

with DAG('clear_xcom_dag', start_date=days_ago(1), schedule_interval=None) as dag:
    clear_xcom_task = PythonOperator(
        task_id='clear_xcom',
        python_callable=clear_xcoms
    )
