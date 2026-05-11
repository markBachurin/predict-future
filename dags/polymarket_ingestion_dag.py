from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
from pss_config.config import DAG_DEFAULT_ARGS

from dags.shared.ingestion_tasks import task_fetch_and_archive, task_validate, task_load_postgres, ensure_schema

logger = logging.getLogger(__name__)


def task_fetch_archive(**context):
    from src.pss.ingestion.polymarket import PolymarketFetcher
    return task_fetch_and_archive(PolymarketFetcher(), **context)



with DAG (
    dag_id = "pss_polymarket_ingestion",
    default_args=DAG_DEFAULT_ARGS,
    description="Layer 1 - ingest active markets from Polymarket",
    schedule_interval="0 */4 * * *",
    start_date=datetime(2026,1,1),
    catchup=False,
    tags=["pss", "ingestion"],
) as dag:
    ensure_schema = PythonOperator(
        task_id="ensure_db_schema",
        python_callable=ensure_schema,
    )

    polymarket_ingestion_archive_s3 = PythonOperator(
        task_id="polymarket_ingestion_archive_s3",
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


    ensure_schema >> polymarket_ingestion_archive_s3 >> validate >> load_to_postgres

