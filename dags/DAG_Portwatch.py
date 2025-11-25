from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# ==== Importações das camadas ====
from scripts.Portwatch_rz import run as run_rz
from scripts.Portwatch_sz import run as run_sz
from scripts.Portwatch_cz import run as run_cz

# ==== Definição da DAG ====

with DAG(
    dag_id="portwatch_medalhao",
    description="Pipeline medalhão Portwatch (rz -> sz -> cz)",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["portwatch", "medalhao"]
) as dag:

    task_rz = PythonOperator(
        task_id="camada_rz",
        python_callable=run_rz
    )

    task_sz = PythonOperator(
        task_id="camada_sz",
        python_callable=run_sz
    )

    task_cz = PythonOperator(
        task_id="camada_cz",
        python_callable=run_cz
    )

    # Ordem de execução
    task_rz >> task_sz >> task_cz
