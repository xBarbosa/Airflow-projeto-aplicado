from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator # type: ignore
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor # type: ignore

from datetime import datetime
import pandas as pd
import re
from io import StringIO

S3_CONN_ID = "aws_conn"
S3_BUCKET = "pa-retail"
POOL_NAME='data_processing_pool'

# Função para ler dados do S3
def read_s3_file(bucket_name, key, s3_conn_id):
    s3_hook = S3Hook(aws_conn_id=s3_conn_id)
    obj = s3_hook.get_key(key, bucket_name)
    return pd.read_csv(obj.get()['Body'])

# Função para salvar dados no S3
def save_to_s3(df, bucket_name, key, s3_conn_id):
    s3_hook = S3Hook(aws_conn_id=s3_conn_id)
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3_hook.load_string(csv_buffer.getvalue(), key, bucket_name, replace=True)

# Função para tratar dados
def process_data(**kwargs):
    bucket_name = S3_BUCKET
    raw_prefix = 'raw'
    trusted_prefix = 'trusted'
    s3_conn_id = kwargs['s3_conn_id']
    file = kwargs['file']
    # files = ['clientes/cliente.csv', 'lojas/loja.csv', 'pagamentos/pagamento.csv', 'produtos/produto.csv', 'vendas/venda.csv', 'vendedores/vendedor.csv']

    # for file in files:
    df = read_s3_file(bucket_name, f'{raw_prefix}/{file}', s3_conn_id)
    
    # Remover linhas em branco e nulas
    df.dropna(inplace=True)
    
    # Remover linhas duplicadas
    df.drop_duplicates(inplace=True)
    
    # Remover caracteres não numéricos de cpf
    if 'cpf' in df.columns:
        df['cpf'] = df['cpf'].apply(lambda x: re.sub(r'\D', '', str(x)))
        
    # Remover caracteres não numéricos de telefones
    if 'celular' in df.columns:
        df['celular'] = df['celular'].apply(lambda x: re.sub(r'[^\d+]', '', str(x)))
    
    # Validar formato de e-mails
    if 'email' in df.columns:
        df = df[df['email'].apply(lambda x: re.match(r'^[\w\.-]+@[\w\.-]+\.\w+$', str(x)) is not None)]
    
    # Salvar dados tratados no S3
    save_to_s3(df, bucket_name, f'{trusted_prefix}/{file}', s3_conn_id)

# Definir DAG
default_args = {
    'owner': 'Rodrigo Barbosa',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG(
    'data_processing_dag',
    default_args=default_args,
    description='Data processing and create trusted data layer',
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=['projeto aplicado', 'trusted', 's3']
)

start = EmptyOperator(task_id='start')
end = EmptyOperator(task_id='end')

files = [
    ('clientes','clientes/clientes.csv'), 
    ('lojas','lojas/lojas.csv'), 
    ('pagamentos','pagamentos/pagamento.csv'), 
    ('produtos','produtos/produto.csv'), 
    ('vendas','vendas/venda.csv'), 
    ('vendedores','vendedores/vendedor.csv')
    ]

for (key, file) in files:
    check_file_sensor = S3KeySensor(
        task_id=f'check_{key}_sensor',
        bucket_key=f'raw/{file}',
        wildcard_match=True,
        bucket_name=S3_BUCKET,
        aws_conn_id=S3_CONN_ID,
        timeout=18*60*60,
        poke_interval=60,
        dag=dag,
        pool=POOL_NAME
    )

    process_data_task = PythonOperator(
        task_id=f'process_{key}_task',
        python_callable=process_data,
        op_kwargs={'s3_conn_id': S3_CONN_ID, 'file': file},
        dag=dag,
        pool=POOL_NAME
    )

    start >> check_file_sensor >> process_data_task >> end
