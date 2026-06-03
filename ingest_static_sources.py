import os
import sys
import json
import pandas as pd
import requests
import dotenv
from google.cloud import bigquery
from google.cloud import storage

# Load secure environment credentials from your hidden .env file
dotenv.load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))

def run_unified_ingestion():
    project_id = os.getenv("GCP_PROJECT_ID")
    bucket_name = os.getenv("GCS_BUCKET_NAME")
    
    if not project_id or not bucket_name:
        print("CRITICAL CONFIG ERROR: Missing credentials parameters in environment vault.")
        sys.exit(1)
        
    # Initialize secure cloud clients
    bq_client = bigquery.Client(project=project_id)
    gcs_client = storage.Client()
    bucket = gcs_client.bucket(bucket_name)
    
  
    print(" [KBA API ENGINE] Fetching Live Vehicle Registry Data Over the Web...")
    
    KBA_ENDPOINT_URL = "https://services-eu1.arcgis.com/U09msXRZoxesNntH/ArcGIS/rest/services/FZ%20Pkw%20mit%20Elektroantrieb%20Zulassungsbezirk/FeatureServer/0/query"
    api_params = {
        "where": "1=1",             # Pull all active German boundaries
        "outFields": "*",           # Request all available metric attributes
        "returnGeometry": "false",  # Skip heavy mapping shapes to prevent script timeout
        "f": "json"                 # Stream back clean relational JSON structures
    }
    headers = {"User-Agent": "DataEngineering-AutomatedPipeline/1.0"}

    try:
        response = requests.get(KBA_ENDPOINT_URL, params=api_params, headers=headers, timeout=30)
        response.raise_for_status()
        raw_payload = response.json()
        
        features = raw_payload.get("features", [])
        record_attributes = [item["attributes"] for item in features]
        
        if not record_attributes:
            raise ValueError("Federal KBA API cluster returned an empty data array.")
            
        kba_df = pd.DataFrame(record_attributes)
        print(f" API Success: Downloaded {len(kba_df)} live regional records from KBA.")
        
        # Normalize European decimal notations if present
        if "Pkw_BEV_Anteil" in kba_df.columns:
            kba_df["Pkw_BEV_Anteil"] = pd.to_numeric(
                kba_df["Pkw_BEV_Anteil"].astype(str).str.replace(",", "."), 
                errors="coerce"
            )
            
        
        print(" Saving a backup historical snapshot to GCS bucket (static_sources/)...")
        kba_blob = bucket.blob("static_sources/kba_vehicle_density.csv")
        kba_blob.upload_from_string(kba_df.to_csv(index=False, sep=";"), content_type="text/csv")
        
        # Load directly to BigQuery Bronze Layer
        kba_table = f"{project_id}.bronze_layer.raw_kba_demand"
        bq_client.load_table_from_dataframe(
            kba_df, 
            kba_table,
            job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        ).result()
        print(f"🎉 [SUCCESS] KBA table perfectly materialized in BigQuery.")
        
    except Exception as e:
        print(f" CRITICAL FAILURE KBA API Extraction failed: {e}")
        sys.exit(1)

    print("\n" + "="*80 + "\n")

    
    print(" [BNETZA GCS ENGINE] Processing Mass Reference Hardware File from Cloud...")
   
    try:
        bnetza_blob = bucket.blob("static_sources/bnetza_registry.csv")
        
        if not bnetza_blob.exists():
            raise FileNotFoundError("Missing manual source file: Please upload bnetza_registry.csv into the 'static_sources' folder.")
            
       
        raw_text = bnetza_blob.download_as_text(encoding="latin-1")
        raw_lines = raw_text.splitlines()
        
        skip_count = 0
        for i, line in enumerate(raw_lines[:30]):
            if ("Betreiber" in line and "Straße" in line) or ("Betreiber" in line and "Ort" in line):
                skip_count = i
                break
        
        print(f" Metadata Guard: Found true data schema. Skipping first {skip_count} introductory lines...")

      
        bnetza_df = pd.read_csv(
            bnetza_blob.open("r", encoding="latin-1"), 
            sep=";", 
            skiprows=skip_count,
            engine="python",       
            on_bad_lines="skip"    
        )
        print(f" Cloud Storage Success: Loaded baseline data rows into memory safely.")
        
        
        bnetza_df = bnetza_df.loc[:, ~bnetza_df.columns.str.contains('^Unnamed')]
        bnetza_df = bnetza_df.dropna(how='all', axis=1)
        
        # Clean up column names to ensure they follow BigQuery naming guidelines
        cleaned_columns = []
        for col in bnetza_df.columns:
            clean = str(col).strip()
            # Strip out trailing commas, semicolons, or punctuation from the raw text line
            clean = clean.rstrip(',;._ ')
            clean = clean.replace(" ", "_")
            clean = clean.replace("/", "_")
            clean = clean.replace("\\", "_")
            clean = clean.replace("[", "").replace("]", "")
            clean = clean.replace("(", "").replace(")", "")
            clean = clean.replace("ä", "ae").replace("ö", "oe").replace("ü", "ue").replace("ß", "ss")
            clean = clean.replace(".", "_")
          
            if not clean:
                clean = "corrupted_empty_column"
            cleaned_columns.append(clean)
            
        bnetza_df.columns = cleaned_columns
        

        bnetza_df = bnetza_df.loc[:, ~bnetza_df.columns.str.contains('corrupted_empty_column')]
        
        power_col = "Nennleistung_kw" if "Nennleistung_kw" in bnetza_df.columns else "Nennleistung_kW"
        if power_col in bnetza_df.columns:
            bnetza_df[power_col] = pd.to_numeric(
                bnetza_df[power_col].astype(str).str.replace(",", "."), 
                errors="coerce"
            )
            
        #
        zip_col = "Postleitzahl" if "Postleitzahl" in bnetza_df.columns else "PLZ"
        if zip_col in bnetza_df.columns:
            bnetza_df[zip_col] = bnetza_df[zip_col].astype(str).str.split('.').str[0].str.zfill(5)
            
        bnetza_table = f"{project_id}.bronze_layer.raw_bnetza_registry"
        
        # Loading  cleaned bulk dataset into BigQuery
        bq_client.load_table_from_dataframe(
            bnetza_df, 
            bnetza_table,
            job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        ).result()
        print(f"🎉 [SUCCESS] {len(bnetza_df):,} BNetzA hardware rows cleaned and sent to BigQuery.")
        
    except Exception as e:
        print(f" [CRITICAL FAILURE] BNetzA cloud storage processing failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    run_unified_ingestion()
    