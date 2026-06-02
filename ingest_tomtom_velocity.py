import os
import sys
import io
import json
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List

import requests
from urllib3.util import Retry
from requests.adapters import HTTPAdapter

from google.cloud import storage
from pyspark.sql import SparkSession

# Load environment configurations
import dotenv
dotenv.load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))

# FORCE SPARK TO USE YOUR EXACT PYTHON RUNTIME (Bypasses Windows Store Blocks)
os.environ["PYSPARK_PYTHON"] = r"C:\Program Files\Python313\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Program Files\Python313\python.exe"

# Setup production-grade structured logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

def create_resilient_session() -> requests.Session:
    """Builds an HTTP session backed by automated backoff retry logic."""
    session = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=2,  # Delays: 2s, 4s, 8s
        status_forcelist=[429, 500, 502, 503, 504],
        raise_on_status=False
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    return session

def harvest_station_telemetry(
    http_client: requests.Session,
    api_key: str,
    current_utc_time: str,
    lat: str,
    lon: str,
    radius: int,
    location_tag: str
) -> List[Dict[str, Any]]:
    """Harvests real-time telemetry from a specific geographic coordinate zone."""
    headers = {"User-Agent": "DataEngineering-EnterprisePipeline/5.0"}
    discovery_url = "https://api.tomtom.com/search/2/categorySearch/Electric Vehicle Charging Station.json"
    disc_params = {"key": api_key, "lat": lat, "lon": lon, "radius": str(radius), "limit": "50"}
    
    logger.info(f"[{location_tag}] Extracting telemetry vectors within {radius}m radius...")
    
    try:
        res = http_client.get(discovery_url, params=disc_params, headers=headers, timeout=15)
        res.raise_for_status()
        discovered_stations = res.json().get("results", [])
        logger.info(f"[{location_tag}] Found {len(discovered_stations)} facilities.")
    except Exception as e:
        logger.error(f"[{location_tag}] Failed to query edge coordinates: {e}")
        return []
        
    availability_url = "https://api.tomtom.com/search/2/chargingAvailability.json"
    local_flat_records = []
    
    for station in discovered_stations:
        sid = station.get("id")
        name = station.get("poi", {}).get("name", "Unknown Station")
        pos = station.get("position", {})
        addr = station.get("address", {})
        
        try:
            avail_res = http_client.get(availability_url, params={"key": api_key, "chargingAvailability": sid}, headers=headers, timeout=10)
            if avail_res.status_code != 200:
                continue
            
            connectors = avail_res.json().get("connectors", [])
            for conn in connectors:
                status = conn.get("availability", {}).get("current", {})
                local_flat_records.append({
                    "ingestion_timestamp": current_utc_time,
                    "tomtom_station_id": sid,
                    "station_name": name,
                    "geo_latitude": float(pos.get("lat")) if pos.get("lat") else None,
                    "geo_longitude": float(pos.get("lon")) if pos.get("lon") else None,
                    "city": addr.get("localName"),
                    "postal_code": addr.get("postalCode"),
                    "connector_type": conn.get("type"),
                    "capacity_total_plugs": int(conn.get("total", 0)),
                    "live_available_plugs": int(status.get("available", 0)),
                    "live_occupied_plugs": int(status.get("occupied", 0)),
                    "live_broken_plugs": int(status.get("outOfService", 0)),
                    "live_reserved_plugs": int(status.get("reserved", 0))
                })
        except Exception:
            continue
            
    return local_flat_records

def run_cloud_native_pyspark_pipeline() -> None:
    """Consolidates data across macro-regional target grids and streams to GCS."""
    current_utc_time = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    logger.info("="*80)
    logger.info(f"[START] Distributed German Macro Grid Telemetry Extraction Strategy")
    logger.info(f"Execution Horizon Reference: {current_utc_time} UTC")
    logger.info("="*80)
    
    API_KEY = os.getenv("TOMTOM_PROD_API_KEY")
    BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
    if not API_KEY or not BUCKET_NAME:
        logger.error("CRITICAL CONFIG ERROR: Missing credentials parameters in environment vault.")
        sys.exit(1)
        
    http_client = create_resilient_session()
    all_aggregated_records: List[Dict[str, Any]] = []
    
    # Geographic matrix targets
    german_regional_targets = [
        {"zone": "HAMBURG_METRO", "lat": "53.5511", "lon": "9.9937", "radius": 25000},
        {"zone": "BERLIN_MUNICIPALITY", "lat": "52.5200", "lon": "13.4050", "radius": 30000},
        {"zone": "FRANKFURT_AM_MAIN", "lat": "50.1109", "lon": "8.6821", "radius": 20000},
        {"zone": "MUNICH_BAVARIA_HUB", "lat": "48.1351", "lon": "11.5820", "radius": 25000},
        {"zone": "COLOGNE_RHEINLAND", "lat": "50.9375", "lon": "6.9603", "radius": 20000},
        {"zone": "STUTTGART_AUTOMOTIVE_ZONE", "lat": "48.7758", "lon": "9.1829", "radius": 20000}
    ]
    
    for target in german_regional_targets:
        records = harvest_station_telemetry(
            http_client=http_client,
            api_key=API_KEY,
            current_utc_time=current_utc_time,
            lat=target["lat"],
            lon=target["lon"],
            radius=target["radius"],
            location_tag=target["zone"]
        )
        all_aggregated_records.extend(records)
        
    if not all_aggregated_records:
        logger.warning("All extraction targets yielded 0 components. Aborting upload routine.")
        sys.exit(1)
        
    logger.info(f"Total compiled macro matrix contains {len(all_aggregated_records)} active connections.")
    
    # Initialize Spark Engine Context
    logger.info("Initializing high-throughput Spark computational cluster environment...")
    spark = SparkSession.builder \
        .appName("Production_EV_Stream_Pipeline") \
        .master("local[*]") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()
        
    try:
        rdd = spark.sparkContext.parallelize([json.dumps(r) for r in all_aggregated_records])
        spark_df = spark.read.json(rdd)
        
        logger.info("Compiling Spark matrix definitions into structured in-memory formats...")
        pandas_df = spark_df.toPandas()
        
        csv_buffer = io.StringIO()
        pandas_df.to_csv(csv_buffer, index=False)
        
        logger.info(f"Establishing encrypted socket stream to GCS: gs://{BUCKET_NAME}...")
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob("staged_imports/staged_live_occupancy.csv")
        
        blob.upload_from_string(csv_buffer.getvalue(), content_type="text/csv")
        logger.info(f" [SUCCESS] Multi-regional macro data stream written cleanly to storage target container.")
        
    except Exception as pipe_err:
        logger.error(f"Transformation or transmission routing breakdown: {pipe_err}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    run_cloud_native_pyspark_pipeline()