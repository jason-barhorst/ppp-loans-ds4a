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
    "load_bq_dataset",
    catchup=False,
    default_args=default_args,
    schedule_interval="@once",
) as dag:
    load_borrower_dim_data = GCSToBigQueryOperator(
        task_id="load_borrower_dim_data",
        bucket=BUCKET_SOURCE,
        source_objects=["bq-tables/borrower_dim.csv"],
        destination_project_dataset_table=f"{DATASET_NAME}.borrower_dim",
        schema_fields=[
            {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "name", "type": "STRING", "mode": "REQUIRED"},
            {"name": "address", "type": "STRING", "mode": "REQUIRED"},
            {"name": "city", "type": "STRING", "mode": "REQUIRED"},
            {"name": "state", "type": "STRING", "mode": "REQUIRED"},
            {"name": "zip_code", "type": "STRING", "mode": "REQUIRED"},
            {"name": "rural_urban_indicator", "type": "STRING", "mode": "REQUIRED"},
            {"name": "naics_code", "type": "STRING", "mode": "REQUIRED"},
            {"name": "employee_count", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "latitude", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "longitude", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "geo_location", "type": "GEOGRAPHY", "mode": "NULLABLE"},
        ],
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
    )

    load_borrower_dim_data
