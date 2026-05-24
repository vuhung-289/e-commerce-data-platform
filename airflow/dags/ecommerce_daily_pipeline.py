"""
DAG: ecommerce_daily_pipeline
Schedule: mỗi ngày lúc 6:00 AM (sau khi các sàn close daily batch)
Flow: ingest_exchange_rate → ingest_news → ingest_transactions
      → dbt_staging → dbt_marts → dbt_test → notify
"""
from datetime import datetime, timedelta, date
import os
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable

# ── Constants ──────────────────────────────────────────────────────────────
GCP_PROJECT   = os.getenv("GCP_PROJECT_ID", "ecommerce-data-platform")
GCS_BUCKET    = os.getenv("GCS_BUCKET_NAME", "ecommerce-raw-data")
DBT_DIR       = Variable.get("DBT_PROJECT_DIR", "/usr/local/airflow/dbt_project")

DEFAULT_ARGS = {
    "owner":            "data-engineer",
    "retries":          3,
    "retry_delay":      timedelta(minutes=5),
    "retry_exponential_backoff": True,   # retry sau 5, 10, 20 phút
    "max_retry_delay":  timedelta(minutes=30),
    "execution_timeout": timedelta(hours=1),
    "email_on_failure": False,           # tuần 3 dùng Slack alert thay email
    "depends_on_past":  False,
}

# ── Helper: lấy execution date ─────────────────────────────────────────────
def get_execution_date(context) -> date:
    """Lấy ngày từ Airflow execution context — quan trọng cho backfill."""
    return context["logical_date"].date()


# ── Task functions ──────────────────────────────────────────────────────────

def ingest_exchange_rate(**context):
    """Task 1: Fetch tỷ giá và upload GCS."""
    sys.path.insert(0, "/usr/local/airflow/include")
    from data_ingestion.sources.exchange_rate import fetch_exchange_rate
    from data_ingestion.utils.gcs_client import GCSClient

    target_date = get_execution_date(context)
    gcs = GCSClient()

    gcs_path = (
        f"exchange_rate/year={target_date.year}/"
        f"month={target_date.month:02d}/"
        f"day={target_date.day:02d}/data.parquet"
    )

    # if gcs.file_exists(gcs_path):
    #     print(f"Already exists, skipping: {gcs_path}")
    #     return {"status": "skipped", "path": gcs_path}

    df = fetch_exchange_rate(target_date)
    path = gcs.upload_parquet(df, gcs_path)
    return {"status": "success", "rows": len(df), "path": path}


def ingest_news(**context):
    """Task 2: Fetch RSS news và upload GCS."""
    sys.path.insert(0, "/usr/local/airflow/include")
    from data_ingestion.sources.news_rss import fetch_news
    from data_ingestion.utils.gcs_client import GCSClient

    target_date = get_execution_date(context)
    gcs = GCSClient()

    gcs_path = (
        f"news/year={target_date.year}/"
        f"month={target_date.month:02d}/"
        f"day={target_date.day:02d}/data.parquet"
    )

    # if gcs.file_exists(gcs_path):
    #     print(f"Already exists, skipping: {gcs_path}")
    #     return {"status": "skipped", "path": gcs_path}

    df = fetch_news(target_date)
    path = gcs.upload_parquet(df, gcs_path)
    return {"status": "success", "rows": len(df), "path": path}


def ingest_transactions(**context):
    """Task 3: Generate synthetic transactions và upload GCS."""
    sys.path.insert(0, "/usr/local/airflow/include")
    from data_ingestion.sources.transactions import generate_transactions
    from data_ingestion.utils.gcs_client import GCSClient

    target_date = get_execution_date(context)
    gcs = GCSClient()

    gcs_path = (
        f"transactions/year={target_date.year}/"
        f"month={target_date.month:02d}/"
        f"day={target_date.day:02d}/data.parquet"
    )

    # if gcs.file_exists(gcs_path):
    #     print(f"Already exists, skipping: {gcs_path}")
    #     return {"status": "skipped", "path": gcs_path}

    df = generate_transactions(target_date, n_records=500)
    path = gcs.upload_parquet(df, gcs_path)
    return {"status": "success", "rows": len(df), "path": path}


def run_dbt_command(command: str, **context):
    import subprocess
    import shutil

    target_date = get_execution_date(context)

    # Tìm dbt binary — fix lỗi "dbt not found" trong shell
    dbt_path = shutil.which("dbt") or "/home/astro/.local/bin/dbt"

    full_command = [
        dbt_path, *command.split(),
        "--project-dir", DBT_DIR,
        "--profiles-dir", DBT_DIR,
        "--vars", f"{{execution_date: {target_date.isoformat()}}}",
    ]

    print(f"Running: {' '.join(full_command)}")
    result = subprocess.run(
        full_command,
        capture_output=True,
        text=True
    )

    print("STDOUT:", result.stdout)
    if result.stderr:
        print("STDERR:", result.stderr)

    if result.returncode != 0:
        raise Exception(
            f"dbt {command} failed (code {result.returncode})\n"
            f"STDERR: {result.stderr}"
        )

    return result.stdout


def check_data_quality(**context):
    """
    Task quality gate: kiểm tra fact_daily_summary có data không.
    Nếu thiếu → branch sang alert task thay vì mark success.
    """
    from google.cloud import bigquery

    target_date = get_execution_date(context)
    client = bigquery.Client(project=GCP_PROJECT)

    query = f"""
        SELECT COUNT(*) as row_count
        FROM `{GCP_PROJECT}.marts.fact_daily_summary`
        WHERE order_date = '{target_date.isoformat()}'
    """

    result = list(client.query(query).result())
    row_count = result[0].row_count

    print(f"Quality check: {row_count} rows for {target_date}")

    if row_count == 0:
        return "alert_missing_data"
    return "pipeline_success"


def send_slack_alert(**context):
    """
    Alert khi pipeline fail hoặc data missing.
    Dùng Slack Incoming Webhook — free, không cần app phức tạp.
    """
    import requests as req

    webhook_url = os.getenv("SLACK_WEBHOOK_URL", "")
    if not webhook_url:
        print("No Slack webhook configured, skipping alert")
        return

    target_date = get_execution_date(context)
    dag_run = context.get("dag_run")

    message = {
        "text": (
            f"🚨 *Pipeline Alert*\n"
            f"DAG: `ecommerce_daily_pipeline`\n"
            f"Date: `{target_date}`\n"
            f"Run ID: `{dag_run.run_id if dag_run else 'unknown'}`\n"
            f"Issue: Missing data in `fact_daily_summary`\n"
            f"Action: Check Airflow logs for details"
        )
    }

    req.post(webhook_url, json=message, timeout=10)
    print("Slack alert sent!")


def log_pipeline_success(**context):
    """Log summary metrics sau khi pipeline hoàn tất."""
    from google.cloud import bigquery

    target_date = get_execution_date(context)
    client = bigquery.Client(project=GCP_PROJECT)

    query = f"""
        SELECT
            order_date,
            total_orders,
            round(gmv_vnd / 1e6, 1)     as gmv_million_vnd,
            round(gmv_usd, 0)            as gmv_usd,
            usd_sell_rate,
            cancellation_rate_pct,
            has_major_ecommerce_event,
            top_category_by_gmv
        FROM `{GCP_PROJECT}.marts.fact_daily_summary`
        WHERE order_date = '{target_date.isoformat()}'
    """

    rows = list(client.query(query).result())
    if rows:
        row = rows[0]
        print(f"""
        ✅ Pipeline Success — {target_date}
        ─────────────────────────────
        Orders:        {row.total_orders}
        GMV (VND):     {row.gmv_million_vnd}M
        GMV (USD):     ${row.gmv_usd:,.0f}
        USD rate:      {row.usd_sell_rate:,.0f}
        Cancel rate:   {row.cancellation_rate_pct}%
        Top category:  {row.top_category_by_gmv}
        News event:    {row.has_major_ecommerce_event}
        """)


# ── DAG Definition ──────────────────────────────────────────────────────────

with DAG(
    dag_id="ecommerce_daily_pipeline",
    description="E-commerce daily ingestion + dbt transformation",
    schedule="0 6 * * *",       # 6:00 AM UTC mỗi ngày = 1:00 PM giờ VN
    start_date=datetime(2026, 5, 11),
    catchup=True,               # True = backfill tự động khi deploy
    max_active_runs=3,          # Chạy tối đa 3 ngày song song khi backfill
    tags=["ecommerce", "ingestion", "dbt", "production"],
    default_args=DEFAULT_ARGS,
    doc_md="""
    ## E-commerce Daily Pipeline

    **Flow:** Ingest → Transform → Test → Notify

    **Sources:**
    - Vietcombank exchange rate API
    - VNExpress / CafeF RSS feeds
    - Synthetic transaction generator

    **Output:** `marts.fact_daily_summary` — daily GMV, USD conversion, news correlation

    **SLA:** Complete within 30 minutes of 6:00 AM UTC
    **Owner:** data-engineer
    """,
) as dag:

    # ── Start ────────────────────────────────────────────────────────────
    start = EmptyOperator(task_id="start")

    # ── Ingestion: chạy song song 3 sources ──────────────────────────────
    t_exchange_rate = PythonOperator(
        task_id="ingest_exchange_rate",
        python_callable=ingest_exchange_rate,
    )

    t_news = PythonOperator(
        task_id="ingest_news",
        python_callable=ingest_news,
    )

    t_transactions = PythonOperator(
        task_id="ingest_transactions",
        python_callable=ingest_transactions,
    )

    # ── dbt Staging: views, chạy sau khi cả 3 ingest xong ───────────────
    t_dbt_staging = PythonOperator(
        task_id="dbt_run_staging",
        python_callable=run_dbt_command,
        op_kwargs={"command": "run --select staging"},
    )

    # ── dbt Marts: incremental ────────────────────────────────────────────
    t_dbt_marts = PythonOperator(
        task_id="dbt_run_marts",
        python_callable=run_dbt_command,
        op_kwargs={"command": "run --select marts"},
    )

    # ── dbt Test: chạy tất cả tests ──────────────────────────────────────
    t_dbt_test = PythonOperator(
        task_id="dbt_test",
        python_callable=run_dbt_command,
        op_kwargs={"command": "test"},
    )

    # ── Quality Gate: branch nếu data missing ────────────────────────────
    t_quality_check = BranchPythonOperator(
        task_id="check_data_quality",
        python_callable=check_data_quality,
    )

    # ── Branch: alert nếu có vấn đề ──────────────────────────────────────
    t_alert = PythonOperator(
        task_id="alert_missing_data",
        python_callable=send_slack_alert,
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    # ── Branch: success path ──────────────────────────────────────────────
    t_success = PythonOperator(
        task_id="pipeline_success",
        python_callable=log_pipeline_success,
    )

    # ── End ──────────────────────────────────────────────────────────────
    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.ONE_SUCCESS,  # chạy dù success hay alert
    )

    # ── Task Dependencies ─────────────────────────────────────────────────
    start >> [t_exchange_rate, t_news, t_transactions]
    [t_exchange_rate, t_news, t_transactions] >> t_dbt_staging
    t_dbt_staging >> t_dbt_marts >> t_dbt_test >> t_quality_check
    t_quality_check >> [t_alert, t_success]
    [t_alert, t_success] >> end