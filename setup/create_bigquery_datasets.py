"""
Create 3 BigQuery datasets corresponding to 3 Lakehouse layers.
Run once during initial setup.
"""
import os
from dotenv import load_dotenv
from google.cloud import bigquery
from loguru import logger

load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
LOCATION = "asia-southeast1"

DATASETS = {
    "raw":     "External tables pointing to GCS Parquet — no data copy",
    "staging": "Cleaned, typed, renamed — 1:1 with raw but standardized",
    "marts":   "Business-ready tables — dbt incremental models",
}

def create_datasets():
    client = bigquery.Client(project=PROJECT_ID)

    for dataset_id, description in DATASETS.items():
        dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{dataset_id}")
        dataset_ref.location = LOCATION
        dataset_ref.description = description

        try:
            client.create_dataset(dataset_ref, exists_ok=True)
            logger.success(f"Dataset created: {PROJECT_ID}.{dataset_id}")
        except Exception as e:
            logger.error(f"Failed to create {dataset_id}: {e}")

if __name__ == "__main__":
    create_datasets()