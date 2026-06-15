import io
import logging
import pandas as pd
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

def load_data():
    logger.info("InitialiSE the  BigQuery Client using  service account JSON key")
    # This path maps perfectly inside your running Docker container environment
    container_key_path = "/opt/airflow/gcp-service-account.json"
    client = bigquery.Client.from_service_account_json(container_key_path)
    
    uri = "gs://ev-pipeline-bucket-amith/staged_imports/staged_live_occupancy.csv"
    table_id = "bronze_layer.raw_live_occupancy"
    
   
    logger.info(f"Fetching target table layout for mapping verification: {table_id}")
    table = client.get_table(table_id)
    target_columns = [field.name for field in table.schema]
    
    logger.info(f"Downloading staging data cache file from data lake: {uri}")
    df = pd.read_csv(uri)
    
    logger.info("Aligning source matrix columns with BigQuery destination layout...")
    df = df[target_columns]
    
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    
    logger.info("Streaming layout-conformed data frames straight to BigQuery storage slot...")
    binary_stream = io.BytesIO(csv_buffer.getvalue().encode('utf-8'))
    load_job = client.load_table_from_file(binary_stream, table_id, job_config=job_config)
    
    load_job.result() 
    logger.info("  Data appended to BigQuery")

if __name__ == "__main__":
    load_data()
