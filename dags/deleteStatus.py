from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.db import provide_session
from airflow.models import TaskInstance, DagRun
from datetime import timedelta
import pendulum

DEFAULT_ARGS = {
    'owner': 'admin',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@provide_session
def cleanup_dag_runs_and_tasks(days: int, session=None):
    """Remove DAG runs and task instances older than a certain number of days with status success or failed."""
    cutoff_date = pendulum.now().subtract(days=days)

    # Deletar DAG Runs com status 'success' e 'failed' que são mais antigos que o cutoff_date
    old_dag_runs = session.query(DagRun).filter(
        DagRun.state.in_(['success', 'failed']),
        DagRun.execution_date < cutoff_date
    )
    deleted_dag_runs_count = old_dag_runs.delete(synchronize_session=False)

    # Deletar Task Instances associadas com status 'success' e 'failed'
    old_task_instances = session.query(TaskInstance).filter(
        TaskInstance.state.in_(['success', 'failed']),
        TaskInstance.execution_date < cutoff_date
    )
    deleted_task_instances_count = old_task_instances.delete(synchronize_session=False)

    # Commit das transações para garantir a remoção dos registros
    session.commit()

    print(f"Deleted {deleted_dag_runs_count} DAG runs and {deleted_task_instances_count} task instances older than {days} days.")

# Definindo a DAG
with DAG(
    dag_id='cleanup_dag_runs_and_tasks',
    default_args=DEFAULT_ARGS,
    description='DAG to cleanup successful and failed DAG runs and task instances older than 3 days.',
    schedule_interval='@daily',  # Executa diariamente
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=['maintenance', 'cleanup']
) as dag:

    cleanup_task = PythonOperator(
        task_id='cleanup_successful_and_failed_runs',
        python_callable=cleanup_dag_runs_and_tasks,
        op_kwargs={'days': 3},  # Mantém apenas 3 dias de histórico
    )

    cleanup_task
