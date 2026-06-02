import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# 🛠️ PRODUCTION CONFIGURATION - Centralized Path Management
# If running on native Windows, use: 'C:\\ev_pipeline'
# If running on WSL/Linux/Docker (Standard), use: '/mnt/c/ev_pipeline'
PROJECT_DIR = '/mnt/c/ev_pipeline'
PYTHON_EXEC = 'python3'

default_args = {
    'owner': 'data_engineering_portfolio',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'ev_live_telemetry_dag',
    default_args=default_args,
    description='High-Frequency Telemetry Ingestion, GCS Landing, and dbt Cloud Curation Model',
    schedule='*/15 * * * *',  # ⏱️ Wakes up precisely every 15 minutes
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['ev_infrastructure', 'telemetry', 'bigquery_production'],
) as dag:

    tomtom_script_path = os.path.join(PROJECT_DIR, 'ingest_tomtom_velocity.py')

    # TASK 1: Trigger the PySpark High-Velocity API Ingestion Script
    execute_pyspark_extraction = BashOperator(
        task_id='pyspark_extract_and_flatten_tomtom',
        bash_command=f'{PYTHON_EXEC} {tomtom_script_path}',
    )

    # TASK 2: Sync and Append the Staged Telemetry Cache to the BigQuery Bronze Dataset Table
    append_cache_to_bronze = BashOperator(
        task_id='bigquery_append_telemetry_cache',
        bash_command='bq load --source_format=CSV --skip_leading_rows=1 --write_disposition=WRITE_APPEND bronze_layer.raw_live_occupancy gs://ev-pipeline-bucket-amith/staged_imports/staged_live_occupancy.csv',
    )

    # TASK 3: Trigger dbt Core Transformations (Profiles directory targeted inside project root)
    trigger_dbt_models = BashOperator(
        task_id='dbt_run_warehouse_transformations',
        bash_command=f'cd {PROJECT_DIR} && dbt run --select mart_demand_gap_analysis --profiles-dir .',
    )

    # TASK 4: Execute dbt Data Quality Validation Verification Tests
    execute_dbt_quality_tests = BashOperator(
        task_id='dbt_execute_schema_tests',
        bash_command=f'cd {PROJECT_DIR} && dbt test --profiles-dir .',
    )

    