from airflow import DAG
from airflow.operators.python_operator import PythonOperator  # type: ignore
from airflow.models import XCom
from airflow.utils.dates import days_ago
from airflow import settings
import sys
import os
# Adiciona o caminho ao sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'include')))
from mssqlconnection import MSSqlConnection

def clientes():
    # Configuração dos parâmetros de conexão
    server = '192.168.0.210\\SQLDEV2019'
    database = 'WideWorldImporters'
    user = 'airflow'
    password = 'airflow'  # Substitua 'your_password' pela senha correta

    # Criação de uma instância da classe DatabaseConnection
    db = MSSqlConnection(server, database, user, password)

    # Conectando ao banco de dados
    db.connect()

    # Executando uma consulta
    query = "SELECT TOP 10 * FROM [WideWorldImporters].[Application].[People] where IsEmployee = 0"
    rows = db.execute_query(query)

    if rows:
        for row in rows:
            print(row)

    # Fechando a conexão
    db.close()


with DAG('conexao_mssql_dag', start_date=days_ago(1), schedule_interval=None) as dag:
    clear_xcom_task = PythonOperator(
        task_id='conexao_mssql',
        python_callable=clientes
    )