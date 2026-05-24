"""
Daily loader: runs all 3 sources and uploads to GCS.
"""
import os
import sys
from datetime import date, timedelta
from pathlib import Path
from dotenv import load_dotenv
from loguru import logger

# Setup path
sys.path.append(str(Path(__file__).parent.parent.parent))
load_dotenv()

from data_ingestion.sources.exchange_rate import fetch_exchange_rate
from data_ingestion.sources.news_rss import fetch_news
from data_ingestion.sources.transactions import generate_transactions
from data_ingestion.utils.gcs_client import GCSClient


# Setup logging
logger.remove()
logger.add(sys.stdout, format="{time:HH:mm:ss} | {level} | {message}")
logger.add(
    "logs/ingestion_{time:YYYY-MM-DD}.log",
    rotation="1 day",
    retention="7 days"
)


def get_gcs_path(source: str, target_date: date) -> str:
    """
    Hive-style partitioning — fast reads for BigQuery and Spark.
    e.g. exchange_rate/year=2024/month=01/day=15/data.parquet
    """
    return (
        f"{source}/"
        f"year={target_date.year}/"
        f"month={target_date.month:02d}/"
        f"day={target_date.day:02d}/"
        f"data.parquet"
    )


def run_daily_ingestion(target_date: date = None, skip_if_exists: bool = True):
    """
    Run full ingestion pipeline for a single day.
    skip_if_exists=True: idempotent — re-runs won't duplicate data.
    """
    if target_date is None:
        target_date = date.today()

    logger.info(f"{'='*50}")
    logger.info(f"Starting daily ingestion for {target_date}")
    logger.info(f"{'='*50}")

    gcs = GCSClient()
    results = {}

    # --- 1. Exchange Rate ---
    logger.info("[1/3] Exchange Rate")
    er_path = get_gcs_path("exchange_rate", target_date)

    if skip_if_exists and gcs.file_exists(er_path):
        logger.info(f"  Skipping (already exists): {er_path}")
    else:
        df_er = fetch_exchange_rate(target_date)
        if not df_er.empty:
            gcs.upload_parquet(df_er, er_path)
            results["exchange_rate"] = {"rows": len(df_er), "path": er_path}

    # --- 2. News RSS ---
    logger.info("[2/3] News RSS")
    news_path = get_gcs_path("news", target_date)

    if skip_if_exists and gcs.file_exists(news_path):
        logger.info(f"  Skipping (already exists): {news_path}")
    else:
        df_news = fetch_news(target_date)
        if not df_news.empty:
            gcs.upload_parquet(df_news, news_path)
            results["news"] = {"rows": len(df_news), "path": news_path}

    # --- 3. Transactions ---
    logger.info("[3/3] Transactions")
    txn_path = get_gcs_path("transactions", target_date)

    if skip_if_exists and gcs.file_exists(txn_path):
        logger.info(f"  Skipping (already exists): {txn_path}")
    else:
        df_txn = generate_transactions(target_date, n_records=500)
        if not df_txn.empty:
            gcs.upload_parquet(df_txn, txn_path)
            results["transactions"] = {"rows": len(df_txn), "path": txn_path}

    # --- Summary ---
    logger.info(f"{'='*50}")
    logger.info("Ingestion complete!")
    for source, info in results.items():
        logger.info(f"  {source}: {info['rows']} rows → {info['path']}")
    logger.info(f"{'='*50}")

    return results


def backfill(days: int = 7):
    """
    Backfill data for the last N days.
    Useful for generating rich data for visualization.
    """
    logger.info(f"Starting backfill for last {days} days")
    today = date.today()

    for i in range(days, 0, -1):
        target = today - timedelta(days=i)
        run_daily_ingestion(target)

    logger.info("Backfill complete!")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="E-commerce Data Ingestion")
    parser.add_argument("--backfill", type=int, default=0,
                        help="Backfill N days (0 = run today)")
    parser.add_argument("--date", type=str, default=None,
                        help="Target date YYYY-MM-DD (default: today)")
    args = parser.parse_args()

    if args.backfill > 0:
        backfill(days=args.backfill)
    else:
        target = date.fromisoformat(args.date) if args.date else date.today()
        run_daily_ingestion(target)