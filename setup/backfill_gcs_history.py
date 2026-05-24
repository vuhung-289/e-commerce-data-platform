import os
import sys
from datetime import date, timedelta
from dotenv import load_dotenv

load_dotenv()

# Add airflow/include to sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.abspath(os.path.join(current_dir, "../airflow/include")))

from data_ingestion.sources.exchange_rate import fetch_exchange_rate
from data_ingestion.sources.transactions import generate_transactions
from data_ingestion.utils.gcs_client import GCSClient
from loguru import logger

def backfill_history():
    gcs = GCSClient()
    start_date = date(2026, 3, 29)
    end_date = date(2026, 5, 20)
    
    current_date = start_date
    while current_date <= end_date:
        logger.info(f"--- Processing {current_date} ---")
        
        # 1. Exchange Rate
        try:
            gcs_path_fx = (
                f"exchange_rate/year={current_date.year}/"
                f"month={current_date.month:02d}/"
                f"day={current_date.day:02d}/data.parquet"
            )
            df_fx = fetch_exchange_rate(current_date)
            gcs.upload_parquet(df_fx, gcs_path_fx)
        except Exception as e:
            logger.error(f"Failed to generate exchange rate for {current_date}: {e}")
            
        # 2. Transactions
        try:
            gcs_path_tx = (
                f"transactions/year={current_date.year}/"
                f"month={current_date.month:02d}/"
                f"day={current_date.day:02d}/data.parquet"
            )
            df_tx = generate_transactions(current_date, n_records=500)
            gcs.upload_parquet(df_tx, gcs_path_tx)
        except Exception as e:
            logger.error(f"Failed to generate transactions for {current_date}: {e}")
            
        current_date += timedelta(days=1)
        
    logger.success("Backfill GCS history completed successfully!")

if __name__ == "__main__":
    backfill_history()
