import pyodbc
from airflow.hooks.mssql_hook import MsSqlHook # type: ignore

class MSSqlConnection:
    def __init__(self, conn_id):
        self.conn_id = conn_id
        self.connection = None

    def connect(self):
        # hook = MsSqlHook(mssql_conn_id='mssql_default')
        # self.connection = hook.get_conn()
        # cursor = conn.cursor()
        # cursor.execute("SELECT 'Essa é a porra da versão >>>>>>> ' + @@VERSION")
        # result = cursor.fetchone()
        # print(result)
        
        # connection_string = (
        #     f'DRIVER={{SQL Server}};'
        #     f'SERVER={self.server};'
        #     f'DATABASE={self.database};'
        #     f'UID={self.user};'
        #     f'PWD={self.password}'
        # )

        try:
            hook = MsSqlHook(mssql_conn_id=self.conn_id) # MsSqlHook(mssql_conn_id='mssql_default')
            self.connection = hook.get_conn()
            # self.connection = pyodbc.connect(connection_string)
            print("Conexão bem-sucedida!")
        except Exception as e:
            print(f"Erro ao conectar ao SQL Server: {e}")
            self.connection = None

    def close(self):
        if self.connection:
            self.connection.close()
            print("Conexão fechada.")
        else:
            print("Nenhuma conexão para fechar.")

    def execute_query(self, query):
        if not self.connection:
            print("Nenhuma conexão ativa. Por favor, conecte-se ao banco de dados primeiro.")
            return None

        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            cursor.close()
            return rows
        except Exception as e:
            print(f"Erro ao executar a consulta: {e}")
            return None
