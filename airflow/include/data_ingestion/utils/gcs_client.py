import os
from pathlib import Path
from dotenv import load_dotenv
from google.cloud import storage
from loguru import logger

load_dotenv()

class GCSClient:
    def __init__(self):
        self.bucket_name = os.getenv("GCS_BUCKET_NAME")
        self.client = storage.Client()
        self.bucket = self.client.bucket(self.bucket_name)
        logger.info(f"Connected to GCS bucket: {self.bucket_name}")

    def upload_parquet(self, df, gcs_path: str) -> str:
        """
        Upload DataFrame as Parquet to GCS.
        e.g. gcs_path: 'exchange_rate/year=2024/month=01/day=15/data.parquet'
        """
        import pyarrow as pa
        import pyarrow.parquet as pq
        import io

        table = pa.Table.from_pandas(df)
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)

        blob = self.bucket.blob(gcs_path)
        blob.upload_from_file(buffer, content_type="application/octet-stream")

        full_path = f"gs://{self.bucket_name}/{gcs_path}"
        logger.success(f"Uploaded {len(df)} rows → {full_path}")
        return full_path

    def file_exists(self, gcs_path: str) -> bool:
        blob = self.bucket.blob(gcs_path)
        return blob.exists()