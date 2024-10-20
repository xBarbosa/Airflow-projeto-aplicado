from airflow import Dataset
from airflow.decorators import dag, task
import pendulum
from airflow.models import Variable
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from include.notifications import notify_teams
from airflow.exceptions import AirflowFailException

import pymssql
import pandas as pd
import requests
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

###
# Connections configurado no UI do Airflow
# mssql_conn_id = "sqlserver_hml"
mssql_conn_id = "sqlserver"

def get_custom_mssql_connection():
    # Recupera a conexão utilizando o MsSqlHook com o mssql_conn_id fornecido
    hook = MsSqlHook(mssql_conn_id=mssql_conn_id)
    conn = hook.get_connection(hook.mssql_conn_id)  # Obtém a conexão com base no ID configurado

    # Estabelece a conexão usando pymssql
    connection = pymssql.connect(
        server=conn.host,
        user=conn.login,
        password=conn.password,
        database=conn.schema,
        tds_version='7.0'  # Aqui você pode ajustar para '8.0' se necessário
    )
    return connection

def process_record(record, cursor):
    try:
        # Pega as informações da lista
        cnpj_emp = record[0]
        dataInicio = record[1]
        dataFim = record[2]

        xml_payload = f"""
        <LinxMicrovix>
            <Authentication user="linx_export" password="linx_export" />
            <ResponseFormat>json</ResponseFormat>
            <IdPortal>14181</IdPortal>
            <Command>
                <Name>LinxMovimento</Name>
                <Parameters>
                    <Parameter id="cnpjEmp">{cnpj_emp}</Parameter>
                    <Parameter id="chave">DADEC98C-CAF9-411C-B8E7-AD98FEFAF9AB</Parameter>
                    <Parameter id="timestamp">0</Parameter>
                    <Parameter id="data_inicial">{dataInicio}</Parameter>
                    <Parameter id="data_fim">{dataFim}</Parameter>
                </Parameters>
            </Command>
        </LinxMicrovix>
        """

        headers = {'Content-Type': 'application/xml'}
        response = requests.post("https://webapi.microvix.com.br/1.0/api/integracao", data=xml_payload, headers=headers)
        response.encoding = 'UTF-8'

        if response.status_code == 200:
            response_text = response.text
            
            # Retirar a última vírgula no JSON dentro do array
            newstr = response_text[:-8] + response_text[-7:]
            texto_formatado = newstr.replace(" ", "").replace("\n", "")
            expected_text = '{"ResponseData":]}'

            if expected_text != texto_formatado:
                struct = json.loads(newstr)

                for item in struct['ResponseData']:
                    data = {
                        'cnpj_emp': item["cnpj_emp"],
                        'documento': item["documento"],
                        'chave_nf': item["chave_nf"],
                        'data_documento': item["data_documento"],
                        'empresa': item["empresa"],
                        'cod_barra': item["cod_barra"],
                        'cancelado': item["cancelado"],
                        'excluido': item["excluido"],
                        'identificador': item["identificador"],
                        'cod_sefaz_situacao': item["cod_sefaz_situacao"],
                        'desc_sefaz_situacao': item["desc_sefaz_situacao"]
                    }

                    insert_sql = f"""
                    INSERT INTO _NOTAS_MICROVIX 
                    (cnpj_emp, documento, chave_nf, data_documento, empresa, cod_barra, cancelado, excluido, identificador, cod_sefaz_situacao, desc_sefaz_situacao) 
                    VALUES 
                    ('{data['cnpj_emp']}', '{data['documento']}', '{data['chave_nf']}', '{data['data_documento']}', '{data['empresa']}', '{data['cod_barra']}', '{data['cancelado']}', '{data['excluido']}', '{data['identificador']}', '{data['cod_sefaz_situacao']}', '{data['desc_sefaz_situacao']}')
                    """
                    # print(insert_sql)
                    cursor.execute(insert_sql)  # Executa o comando SQL para INSERT

        return f"Success: {cnpj_emp}"

    except Exception as e:
        return f"Failed: {cnpj_emp}, Error: {str(e)}"


@task
def get_data_from_sql_and_insert(**context):
    connection = get_custom_mssql_connection()  # Conexão criada uma vez
    cursor = connection.cursor()  # Cursor criado uma vez

    try:
        # Fetch data from the database
        truncate_sql = f"""TRUNCATE TABLE _NOTAS_MICROVIX"""
        cursor.execute(truncate_sql)
        
        cursor.execute(f"""          
                SELECT 
                    cast(B.TaxIdNum as varchar(30)) AS cnpjEmp, 
                    CAST(DATEADD(DAY, -2, GETDATE()) AS DATE) as DtInicio, 
                    CAST(DATEADD(DAY, -1, GETDATE()) AS DATE) as DtFim
                FROM F2N_AUX_OBPL B WITH(NOLOCK)

                order by 1 asc
            """)  # Substitua 'your_table' pelo nome da sua tabela
            # where B.TaxIdNum = 08743025010311 
        cnpjs_records = cursor.fetchall()

        # if not cnpjs_records:
        #     raise ValueError("Nenhum dado foi retornado da consulta SQL.")


        batch_size = 1
        for i in range(0, len(cnpjs_records), batch_size):
            batch = cnpjs_records[i:i + batch_size]

            # Use ThreadPoolExecutor to process records in parallel
            with ThreadPoolExecutor(max_workers=15) as executor:
                futures = [executor.submit(process_record, record, cursor) for record in batch]
                for future in as_completed(futures):
                    try:
                        result = future.result()
                        print(result)  # Log the result
                        
                    except Exception as e:
                        print(f"Erro ao processar record: {e}")

            connection.commit()  # Commit de todas as transações

    except Exception as e:
        raise AirflowFailException(str(e))

    finally:
        cursor.close()  # Fechar o cursor
        connection.close()  # Fechar a conexão ao final de tudo


# Define the DAG
@dag(
    start_date= pendulum.datetime(2024, 1, 1, tz="America/Sao_Paulo"),
    schedule_interval="*/30 * * * *",  # Executa a cada 10 minutos
    # schedule_interval='@daily',
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Granado", "retries": 3},
    on_failure_callback=notify_teams,
    tags=["Produção - carga"]
)
def CargaETLSql():

    # Instancia a task de execução
    get_data_from_sql_and_insert()

CargaETLSql()
