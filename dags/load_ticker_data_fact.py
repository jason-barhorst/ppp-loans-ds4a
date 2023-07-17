import datetime

from airflow import models

from airflow.providers.google.cloud.operators import bigquery

from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)

YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)
DATASET_NAME = "ppp_loan_dataset"
PROJECT_ID = "mystic-gradient-387720"
BUCKET_SOURCE = "sample-bucket-d4sa-data"
TABLE_NAME = "ticker_data_fact"

schema_fields = [
    {"name": "ticker", "type": "STRING", "mode": "REQUIRED"},
    {"name": "date", "type": "DATE", "mode": "REQUIRED"},
    {"name": "open", "type": "FLOAT64", "mode": "REQUIRED"},
    {"name": "high", "type": "FLOAT64", "mode": "REQUIRED"},
    {"name": "low", "type": "FLOAT64", "mode": "REQUIRED"},
    {"name": "close", "type": "FLOAT64", "mode": "REQUIRED"},
    {"name": "volume", "type": "FLOAT64", "mode": "REQUIRED"},
]

default_args = {
    "owner": "Team Eigen",
    "depends_on_past": False,
    "email": [""],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "start_date": YESTERDAY,
}

with models.DAG(
    f"load_{TABLE_NAME}",
    catchup=False,
    default_args=default_args,
    schedule_interval=None,
) as dag:
    load_ticker_data_fact = GCSToBigQueryOperator(
        task_id=f"load_{TABLE_NAME}",
        bucket=BUCKET_SOURCE,
        source_objects=[f"bq-tables/{TABLE_NAME}.csv"],
        destination_project_dataset_table=f"{DATASET_NAME}.{TABLE_NAME}",
        schema_fields=schema_fields,
        # skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
    )

    load_ticker_data_fact
