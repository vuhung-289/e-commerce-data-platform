import os
from dotenv import load_dotenv
from google.cloud import bigquery
from loguru import logger

load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")

EXTERNAL_TABLES = [
    {
        "table_id": f"{PROJECT_ID}.raw.transactions",
        "source_uri": f"gs://{BUCKET_NAME}/transactions/*",
    },
    {
        "table_id": f"{PROJECT_ID}.raw.exchange_rate",
        "source_uri": f"gs://{BUCKET_NAME}/exchange_rate/*",
    },
    {
        "table_id": f"{PROJECT_ID}.raw.news",
        "source_uri": f"gs://{BUCKET_NAME}/news/*",
    },
]


def create_external_table(client: bigquery.Client, config: dict):
    external_config = bigquery.ExternalConfig("PARQUET")
    external_config.source_uris = [config["source_uri"]]
    external_config.autodetect = True  # auto-detect types from Parquet

    hive_options = bigquery.HivePartitioningOptions()
    hive_options.mode = "AUTO"
    hive_options.source_uri_prefix = config["source_uri"].replace("/*", "")
    external_config.hive_partitioning = hive_options

    table = bigquery.Table(config["table_id"])  # no hardcoded schema
    table.external_data_configuration = external_config

    client.delete_table(config["table_id"], not_found_ok=True)  # drop old table
    client.create_table(table)
    logger.success(f"Recreated: {config['table_id']}")


def main():
    client = bigquery.Client(project=PROJECT_ID)
    for config in EXTERNAL_TABLES:
        try:
            create_external_table(client, config)
        except Exception as e:
            logger.error(f"Failed: {config['table_id']} — {e}")


if __name__ == "__main__":
    main()