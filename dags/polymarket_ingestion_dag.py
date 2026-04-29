from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging

from dags.shared.ingestion_tasks import task_fetch_and_archive, task_validate, task_load_postgres

logger = logging.getLogger(__name__)


def task_fetch_archive(**context):
    from src.pss.ingestion.polymarket import PolymarketFetcher
    return task_fetch_and_archive(PolymarketFetcher(), **context)

default_args = {
    "owner" : "pss",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG (
    dag_id = "pss_polymarket_ingestion",
    default_args=default_args,
    description="Layer 1 - ingest active markets from Polymarket and Kalshi",
    schedule_interval=timedelta(minutes=15),
    start_date=datetime(2026,1,1),
    catchup=False,
    tags=["pss", "ingestion"],
) as dag:
    polymarket_ingestion_archive_s3 = PythonOperator(
        task_id="polymarket_ingest_archive_s3",
        python_callable=task_fetch_archive,
    )

    validate = PythonOperator(
        task_id="validate",
        python_callable=task_validate,
    )

    load_to_postgres = PythonOperator(
        task_id="load_to_postgres",
        python_callable=task_load_postgres
    )


    polymarket_ingestion_archive_s3 >> validate >> load_to_postgres

