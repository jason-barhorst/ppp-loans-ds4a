import datetime

from airflow import models

from airflow.providers.google.cloud.operators import bigquery

YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)
DATASET_NAME = "ppp_loan_dataset"
PROJECT_ID = "mystic-gradient-387720"


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
    "create_bq_dataset",
    catchup=False,
    default_args=default_args,
    schedule_interval="@once",
) as dag:
    # Print the dag_run id from the Airflow logs
    create_bq_dataset = bigquery.BigQueryCreateEmptyDatasetOperator(
        task_id="create_bq_dataset", project_id=PROJECT_ID, dataset_id=DATASET_NAME
    )

    financial_institution_dim = bigquery.BigQueryCreateEmptyTableOperator(
        task_id="financial_institution_dim",
        dataset_id=DATASET_NAME,
        table_id="financial_institution_dim",
        schema_fields=[
            {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "cert", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "name", "type": "STRING", "mode": "REQUIRED"},
            {"name": "address", "type": "STRING", "mode": "REQUIRED"},
            {"name": "state", "type": "STRING", "mode": "REQUIRED"},
            {"name": "city", "type": "STRING", "mode": "REQUIRED"},
            {"name": "date_updt", "type": "DATE", "mode": "REQUIRED"},
            {"name": "active", "type": "BOOL", "mode": "REQUIRED"},
            {"name": "insfdic", "type": "BOOL", "mode": "REQUIRED"},
            {"name": "offices", "type": "STRING", "mode": "REQUIRED"},
        ],
    )

    ticker_data_fact = bigquery.BigQueryCreateEmptyTableOperator(
        task_id="ticker_data_fact",
        dataset_id=DATASET_NAME,
        table_id="ticker_data_fact",
        schema_fields=[
            {"name": "id_ticker", "type": "STRING", "mode": "REQUIRED"},
            {"name": "id_data", "type": "DATE", "mode": "REQUIRED"},
            {"name": "open", "type": "FLOAT64", "mode": "REQUIRED"},
            {"name": "close", "type": "FLOAT64", "mode": "REQUIRED"},
            {"name": "high", "type": "FLOAT64", "mode": "REQUIRED"},
            {"name": "low", "type": "FLOAT64", "mode": "REQUIRED"},
            {"name": "volume", "type": "FLOAT64", "mode": "REQUIRED"},
        ],
    )

    summary_deposit_fact = bigquery.BigQueryCreateEmptyTableOperator(
        task_id="summary_deposit_fact",
        dataset_id=DATASET_NAME,
        table_id="summary_deposit_fact",
        schema_fields=[
            {"name": "cert", "type": "STRING", "mode": "REQUIRED"},
            {"name": "bank_name", "type": "STRING", "mode": "REQUIRED"},
            {"name": "asset", "type": "STRING", "mode": "REQUIRED"},
            {"name": "total_domestics_deposits", "type": "NUMERIC", "mode": "REQUIRED"},
            {"name": "total_deposits", "type": "NUMERIC", "mode": "REQUIRED"},
            {"name": "insured", "type": "STRING", "mode": "REQUIRED"},
        ],
    )

    loan_data_fact = bigquery.BigQueryCreateEmptyTableOperator(
        task_id="loan_data_fact",
        dataset_id=DATASET_NAME,
        table_id="loan_data_fact",
        schema_fields=[
            {"name": "id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "borrow_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "lender_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "loan_number", "type": "STRING", "mode": "REQUIRED"},
            {"name": "loan_amount", "type": "STRING", "mode": "REQUIRED"},
            {"name": "loan_status", "type": "STRING", "mode": "REQUIRED"},
            {"name": "loan_status_date_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "forgiveness_amount", "type": "FLOAT64", "mode": "REQUIRED"},
            {"name": "forgiveness_date", "type": "DATETIME", "mode": "REQUIRED"},
        ],
    )

    borrower_dim = bigquery.BigQueryCreateEmptyTableOperator(
        task_id="borrower_dim",
        dataset_id=DATASET_NAME,
        table_id="borrower_dim",
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
    )

    date_dim = bigquery.BigQueryCreateEmptyTableOperator(
        task_id="date_dim",
        dataset_id=DATASET_NAME,
        table_id="date_dim",
        schema_fields=[
            {"name": "id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "year", "type": "STRING", "mode": "REQUIRED"},
            {"name": "month", "type": "STRING", "mode": "REQUIRED"},
            {"name": "quarter", "type": "STRING", "mode": "REQUIRED"},
            {"name": "day", "type": "STRING", "mode": "REQUIRED"},
            {"name": "date", "type": "DATE", "mode": "REQUIRED"},
        ],
    )

    failed_bank_data_dim = bigquery.BigQueryCreateEmptyTableOperator(
        task_id="failed_bank_data_dim",
        dataset_id=DATASET_NAME,
        table_id="failed_bank_data_dim",
        schema_fields=[
            {"name": "cert", "type": "STRING", "mode": "REQUIRED"},
            {"name": "bank_name", "type": "STRING", "mode": "REQUIRED"},
            {"name": "id_closing_date", "type": "STRING", "mode": "REQUIRED"},
            {"name": "fund", "type": "STRING", "mode": "REQUIRED"},
        ],
    )

    create_bq_dataset >> financial_institution_dim
    create_bq_dataset >> borrower_dim
    create_bq_dataset >> summary_deposit_fact
    create_bq_dataset >> loan_data_fact
    create_bq_dataset >> failed_bank_data_dim
    create_bq_dataset >> date_dim
    create_bq_dataset >> ticker_data_fact
