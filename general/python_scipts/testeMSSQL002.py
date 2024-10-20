from airflow import DAG
from airflow.operators.python_operator import PythonOperator # type: ignore
from airflow.hooks.mssql_hook import MsSqlHook # type: ignore
from datetime import datetime

import sys
import os
# Adiciona o caminho ao sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'include')))
from mssqlconnection import MSSqlConnection

def query_sql_server():
     # Criação de uma instância da classe DatabaseConnection
    db = MSSqlConnection('mssql_default')
    conn = db.connect()
    
    query = "SELECT * FROM [PARetail].[dbo].[Clientes]"
    rows = db.execute_query(query)
    
    print(rows)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 0,
}

dag = DAG(
    'teste_conn_mssql_example',
    default_args=default_args,
    schedule_interval=None,
)

run_query = PythonOperator(
    task_id='run_query',
    python_callable=query_sql_server,
    dag=dag,
)