from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
from pss_config.config import DAG_DEFAULT_ARGS
from src.pss.llm.classifier import MarketClassifier
from src.pss.llm.client import  LLMClient
from src.pss.storage.postgres.client import  PostgresClient
from datetime import timedelta, datetime
from dags.shared.ingestion_tasks import ensure_schema
import asyncio

logging.basicConfig(level=logging.INFO)

def classify(**context):
    pg_client = PostgresClient()
    llm_client = LLMClient()
    classifier = MarketClassifier(llm_client, pg_client)
    classifier.semaphore1 = asyncio.Semaphore(1)
    classifier.semaphore2 = asyncio.Semaphore(1)

    return asyncio.run(classifier.classify_all())


with DAG (
    dag_id="pss_classify_markets",
    default_args=DAG_DEFAULT_ARGS,
    description= "Layer 2 - classify all unprocessed ingested markets",
    schedule_interval="30 */4 * * *",
    start_date = datetime(2026,1,1),
    catchup=False,
    tags=["pss", "classification"]
) as dag:
    ensure_schema = PythonOperator(
        task_id="ensure_db_schema",
        python_callable=ensure_schema,
    )

    classify_markets = PythonOperator(
        task_id="classify_markets",
        python_callable=classify,
    )

    ensure_schema >> classify_markets
