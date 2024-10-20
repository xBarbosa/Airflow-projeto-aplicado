from airflow import DAG
from airflow.decorators import task
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import pendulum
import pymssql

def get_custom_mssql_connection():
    # Recupera a conexão utilizando o MsSqlHook com o mssql_conn_id fornecido
    hook = MsSqlHook(mssql_conn_id='sqlserver')
    conn = hook.get_connection(hook.mssql_conn_id)  # Obtém a conexão com base no ID configurado

    # Estabelece a conexão usando pymssql
    connection = pymssql.connect(
        server=conn.host,
        user=conn.login,
        password=conn.password,
        database=conn.schema,
        tds_version='7.0'
    )
    return connection

# Definindo o DAG
with DAG(
    dag_id='custom_mssql_dag',
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Sao_Paulo"),
    schedule_interval='@daily',
    catchup=False,
    tags=['SQL Server', 'Custom Connection']
) as dag:

    @task
    def execute_sql():
        connection = get_custom_mssql_connection()
        cursor = connection.cursor()
        cursor.execute(f"""          
        select count(1) from  _NOTAS_MICROVIX with(nolock)
        where 1=1
        """)  # Substitua 'your_table' pelo nome da sua tabela
        data = cursor.fetchall()
        print(f"Data fetched: {data}")  # Exibindo os dados no log


        cursor.execute("""
        INSERT INTO _NOTAS_MICROVIX (cnpj_emp, documento, chave_nf, data_documento, empresa, cod_barra, cancelado, excluido, identificador, cod_sefaz_situacao, desc_sefaz_situacao) VALUES ('08743025000430', '10958', '33240808743025000430650080000109581329491228', '14/08/2024 00:00:00', '4', '7896512946140', 'N', 'N', '59dc6101-2375-438f-9b0b-c5b4af8bb15f', '1', 'Autorizada')
        """)
        # Commit das alterações para operações de INSERT/UPDATE
        connection.commit()
        
        connection.close()
    # Definindo a ordem de execução das tarefas
    execute_sql()
