from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os

# Definir argumentos padrão do DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 1),
    'retries': 3, 
    'retry_delay': timedelta(minutes=2),  
    'email_on_failure': False,  # Possível melhoria
}

def check_file_exists(filepath):
    """
    Verifica se um arquivo existe no caminho especificado.

    Parâmetros:
    - filepath (str): Caminho absoluto ou relativo do arquivo a ser verificado.
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Arquivo não encontrado: {filepath}")

# Criar a DAG
with DAG(
    'etl_pipeline',
    default_args=default_args,
    description='Pipeline ETL com Bronze, Silver e Gold',
    schedule_interval=timedelta(days=1),
    catchup=False
) as dag:

    check_bronze_file = PythonOperator(
        task_id="check_bronze_file",
        python_callable=check_file_exists,
        op_args=['/opt/airflow/dags/bronze.py']
    )

    check_silver_file = PythonOperator(
        task_id="check_silver_file",
        python_callable=check_file_exists,
        op_args=['/opt/airflow/dags/silver.py']
    )

    check_gold_file = PythonOperator(
        task_id="check_gold_file",
        python_callable=check_file_exists,
        op_args=['/opt/airflow/dags/gold.py']
    )

    # Tarefa 1: Executar o Bronze
    bronze_task = BashOperator(
        task_id='bronze_task',
        bash_command='python /opt/airflow/dags/bronze.py',
    )

    # Tarefa 2: Executar o Silver
    silver_task = BashOperator(
        task_id='silver_task',
        bash_command='python /opt/airflow/dags/silver.py',
    )

    # Tarefa 3: Executar o Gold
    gold_task = BashOperator(
        task_id='gold_task',
        bash_command='python /opt/airflow/dags/gold.py',
    )

    # Definir a ordem das execuções com validações antes das tarefas
    check_bronze_file >> bronze_task
    check_silver_file >> silver_task
    check_gold_file >> gold_task

    bronze_task >> silver_task >> gold_task
