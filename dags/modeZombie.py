from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.db import provide_session
from airflow.models import TaskInstance
from datetime import datetime, timedelta
import pendulum

DEFAULT_ARGS = {
    'owner': 'admin',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@provide_session
def find_and_fail_zombie_tasks(timeout_minutes: int, session=None):
    """Identifica tasks em modo zumbi e as marca como falhas."""
    # Tempo limite para uma task ser considerada zumbi
    cutoff_time = pendulum.now().subtract(minutes=timeout_minutes)

    # Consulta para buscar Task Instances que estão "running" além do tempo limite
    zombie_tasks = session.query(TaskInstance).filter(
        TaskInstance.state == 'running',
        TaskInstance.start_date < cutoff_time
    ).all()

    for task in zombie_tasks:
        print(f"Marcando task zumbi como falha: {task.dag_id} - {task.task_id} - {task.execution_date}")
        task.set_state('failed', session=session)  # Marca a task como falha

    # Commit para persistir as mudanças no banco de dados
    session.commit()
    print(f"Total de {len(zombie_tasks)} tasks zumbis marcadas como falhas.")

# Definindo a DAG
with DAG(
    dag_id='cleanup_zombie_tasks',
    default_args=DEFAULT_ARGS,
    description='DAG para marcar tasks zumbis como falhas.',
    schedule_interval='@hourly',  # Executa a cada hora
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=['maintenance', 'zombie', 'cleanup']
) as dag:

    detect_and_fail_zombies = PythonOperator(
        task_id='detect_and_fail_zombies',
        python_callable=find_and_fail_zombie_tasks,
        op_kwargs={'timeout_minutes': 60},  # Considera tasks zumbis após 60 minutos de execução
    )

    detect_and_fail_zombies
