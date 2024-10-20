from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator # type: ignore
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from datetime import datetime

# connections
S3_CONN_ID = "aws_conn"
MSSQL_CONN_ID = "mssql_default"
S3_BUCKET = "pa-retail"
POOL_NAME='data_processing_pool'

# Configurações do DAG
default_args = {
    'owner': 'Rodrigo Barbosa',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG(
    'move_data_sql_to_s3',
    default_args=default_args,
    description='Move data from SQL Server to S3',
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=['projeto aplicado', 'raw', 'mssql', 's3']
)

start = EmptyOperator(task_id='start')
end = EmptyOperator(task_id='end')

files = [
    ('clientes','clientes/clientes.csv', 'uvw_clientes'), 
    ('lojas','lojas/lojas.csv', 'uvw_filiais'), 
    ('pagamentos','pagamentos/pagamento.csv', 'uvw_pagamento'), 
    ('produtos','produtos/produto.csv', 'uvw_produtos'), 
    ('vendas','vendas/venda.csv', 'uvw_vendas'), 
    ('vendedores','vendedores/vendedor.csv', 'uvw_vendedores')
    ]

for (key, file, table) in files:
    move_data_to_s3 = SqlToS3Operator(
        task_id=f'move_{key}_data_to_s3',
        sql_conn_id=MSSQL_CONN_ID,
        query=f'SELECT * FROM [PARetail].[dbo].[{table}]',
        s3_bucket=S3_BUCKET,
        s3_key=f'raw/{file}',
        aws_conn_id=S3_CONN_ID,
        replace=True,
        dag=dag,
        pool=POOL_NAME
    )
    
    start >> move_data_to_s3 >> end

   
# # Operador para mover dados de clientes para o S3
# move_client_data_to_s3 = SqlToS3Operator(
#     task_id='move_client_data_to_s3',
#     sql_conn_id=MSSQL_CONN_ID,
#     query='SELECT * FROM [PARetail].[dbo].[uvw_clientes]',
#     s3_bucket=S3_BUCKET,
#     s3_key='raw/clientes/cliente.csv',
#     aws_conn_id=S3_CONN_ID,
#     replace=True,
#     dag=dag,
# )

# # Operador para mover dados de produtos para o S3
# move_product_data_to_s3 = SqlToS3Operator(
#     task_id='move_product_data_to_s3',
#     sql_conn_id=MSSQL_CONN_ID,
#     query='SELECT * FROM [PARetail].[dbo].[uvw_produtos]',
#     s3_bucket=S3_BUCKET,
#     s3_key='raw/produtos/produto.csv',
#     aws_conn_id=S3_CONN_ID,
#     replace=True,
#     dag=dag,
# )

# # Operador para mover dados de lojas para o S3
# move_banch_data_to_s3 = SqlToS3Operator(
#     task_id='move_branch_data_to_s3',
#     sql_conn_id=MSSQL_CONN_ID,
#     query='SELECT * FROM [PARetail].[dbo].[uvw_filiais]',
#     s3_bucket=S3_BUCKET,
#     s3_key='raw/lojas/loja.csv',
#     aws_conn_id=S3_CONN_ID,
#     replace=True,
#     dag=dag,
# )

# # Operador para mover dados de vendedores para o S3
# move_salesperson_data_to_s3 = SqlToS3Operator(
#     task_id='move_salesperson_data_to_s3',
#     sql_conn_id=MSSQL_CONN_ID,
#     query='SELECT * FROM [PARetail].[dbo].[uvw_vendedores]',
#     s3_bucket=S3_BUCKET,
#     s3_key='raw/vendedores/vendedor.csv',
#     aws_conn_id=S3_CONN_ID,
#     replace=True,
#     dag=dag,
# )


# # Operador para mover dados de movimento de venda para o S3
# move_sales_data_to_s3 = SqlToS3Operator(
#     task_id='move_sales_data_to_s3',
#     sql_conn_id=MSSQL_CONN_ID,
#     query='select * from [PARetail].[dbo].[uvw_vendas]',
#     s3_bucket=S3_BUCKET,
#     s3_key='raw/vendas/venda.csv',
#     aws_conn_id=S3_CONN_ID,
#     replace=True,
#     dag=dag,
# )

# # Operador para mover dados de pagamento das vendas para o S3
# move_payment_data_to_s3 = SqlToS3Operator(
#     task_id='move_payment_data_to_s3',
#     sql_conn_id=MSSQL_CONN_ID,
#     query='SELECT * FROM [PARetail].[dbo].[uvw_pagamento]',
#     s3_bucket=S3_BUCKET,
#     s3_key='raw/pagamentos/pagamento.csv',
#     aws_conn_id=S3_CONN_ID,
#     replace=True,
#     dag=dag,
# )

# # Fluxo de execução
# start >> move_client_data_to_s3 >> end
# start >> move_product_data_to_s3 >> end
# start >> move_banch_data_to_s3 >> end
# start >> move_salesperson_data_to_s3 >> end
# start >> move_sales_data_to_s3 >> end
# start >> move_payment_data_to_s3 >> end