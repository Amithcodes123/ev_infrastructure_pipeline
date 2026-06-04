import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator



PROJECT_DIR = '/opt/airflow'
PYTHON_EXEC = 'python3'

default_args = {
    'owner': 'data_engineering_portfolio',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'ev_static_dimensions_dag',
    default_args=default_args,
    description='Quarterly Automation Line for KBA API and BNetzA Bulk File Synchronization',
    schedule='0 0 1 */3 *', 
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['ev_infrastructure', 'dimensions', 'quarterly_sync'],
) as dag:

    static_script_path = os.path.join(PROJECT_DIR, 'ingest_static_sources.py')

    execute_unified_static_ingestion = BashOperator(
        task_id='bigquery_materialize_static_foundations',
        bash_command=f'{PYTHON_EXEC} {static_script_path}',
    )
