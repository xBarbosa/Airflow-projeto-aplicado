from airflow import Dataset
from airflow.decorators import dag, task
from pendulum import datetime
from airflow.models import Variable
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from include.notifications import notify_teams
from airflow.exceptions import AirflowFailException

import pandas as pd
import requests
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

###
# Connections configurado no UI do Airflow
mssql_conn_id = "sqlserver"

###
# Define a function to process each CNPJ
def process_record(record):
    try:
        mssql_hook = MsSqlHook(mssql_conn_id=mssql_conn_id) ##Conecta no banco pelo hook do airflow
        
        ## - Pega as informações da lista
        cnpj_emp  = record[0]
        data      = record[1]
        documento = record[2]



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
                    <Parameter id="data_inicial">{data}</Parameter>
                    <Parameter id="data_fim">{data}</Parameter>
                    <Parameter id="documento">{documento}</Parameter>
                </Parameters>
            </Command>
        </LinxMicrovix>
        """

        headers = {'Content-Type': 'application/xml'}
        response = requests.post("https://webapi.microvix.com.br/1.0/api/integracao", data=xml_payload, headers=headers)
        response.encoding = 'UTF-8'

        if response.status_code == 200:
            response_text = response.text
            
            ######
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
                    }

                    ###### - Comando SQL
                    ## - Insert
                    insert_sql = f"""
                    INSERT INTO _CHAVE_ACESSO_LINX 
                    (cnpj_emp, documento, chave_nf, data_documento, empresa) 
                    VALUES 
                    ('{data['cnpj_emp']}', '{data['documento']}', '{data['chave_nf']}', '{data['data_documento']}', '{data['empresa']}')
                    """
                    mssql_hook.run(insert_sql) ## Executa o camando SQL para INSERT
                    
                    ## - Delete
                    delete_sql = f"""
                    WITH CTE_Duplicates AS (
                        SELECT 
                            documento,
                            ROW_NUMBER() OVER (PARTITION BY chave_nf ORDER BY (chave_nf)) AS row_num
                        FROM 
                            _CHAVE_ACESSO_LINX
                    )
                    DELETE FROM CTE_Duplicates
                    WHERE row_num > 1"""
                    mssql_hook.run(delete_sql)
                    
                    # ## - Update
                    # update_sql = f"""
                    #         UPDATE A 
                    #           SET A.U_CHAVEACESSO = B.chave_nf, A.QUEUE_STATUS = ('U')
                    #         FROM F2N_INT_DOC_HEADER A
                    #         INNER JOIN _CHAVE_ACESSO_LINX b
                    #           on a.serial = b.documento
                    #           and Convert(varchar(10),a.docDate,103) = Convert(varchar(10),b.data_documento,103)
                    #           and a.BPlId = b.empresa
                    #         WHERE 1=1
                    #         and A.QUEUE_STATUS = ('F')
                    #         AND A.SYS_RET_MSG  LIKE '%CHAVE DE ACESSO%'  
                    #         AND b.chave_nf <> ' '
                    # """
                    # mssql_hook.run(update_sql) ## Executa o camando SQL para UPDATE


        return f"Success: {cnpj_emp}"

    except Exception as e:
        return f"Failed: {cnpj_emp}, Error: {str(e)}"


############
######
# Define the basic parameters of the DAG, like schedule and start_date
@dag(
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    # schedule_interval="*/10 * * * *",  # Executa a cada 10 minutos
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Granado", "retries": 3},
    on_failure_callback=notify_teams,
    tags=["Produção - erro"]
)
def ETLChaveDeAcessoSql():
    ######
    
    # Task to fetch data from SQL Server and insert it back after processing
    @task
    def get_data_from_etl_Chave_de_acesso(**context):
      try:   
        mssql_hook = MsSqlHook(mssql_conn_id=mssql_conn_id)  # Connection ID configured in Airflow

        # ## - truncate
        # truncate_sql = f"""TRUNCATE TABLE _CHAVE_ACESSO_LINX"""
        # mssql_hook.run(truncate_sql)## Executa o camando SQL para TRUNCAR a tabela
      
        # Fetch data from the database
        cnpjs_records  = mssql_hook.get_records(
            sql=f"""SELECT cast(B.TaxIdNum as varchar(30)) AS cnpjEmp, A.TaxDate as data_documento, A.SERIAL as documento 
            FROM F2N_INT_DOC_HEADER  A WITH(NOLOCK) 
            INNER JOIN F2N_AUX_OBPL B WITH(NOLOCK) 
            ON A.BPLiD = B.BPLiD 
            WHERE 1=1
            AND A.SYS_RET_MSG  LIKE '%Chave de acesso%'
            and A.QUEUE_STATUS IN ('F')
            order by 2 asc"""
        )

        if not cnpjs_records:
            raise ValueError("Nenhum dado foi retornado da consulta SQL.")
            
      except Exception as e:
        raise AirflowFailException()   

      # Use ThreadPoolExecutor to process records in parallel
      with ThreadPoolExecutor(max_workers=15) as executor:
          futures = [executor.submit(process_record, record) for record in cnpjs_records]
          for future in as_completed(futures):
              try:
                  result = future.result()
                  print(result)  # Optional: Log the result
              except Exception as e:
                  print(f"Erro ao processar record: {e}")

         

    get_data_from_etl_Chave_de_acesso()

#
# Instantiate the DAG
ETLChaveDeAcessoSql()