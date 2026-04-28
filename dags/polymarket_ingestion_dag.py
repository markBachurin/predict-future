from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging

logger = logging.getLogger(__name__)



def run_polymarket_ingestion(**context):
    from src.pss.ingestion.polymarket import PolymarketFetcher
    from src.pss.storage.postgres.client import get_conn, upsert_raw_market, upsert_market, insert_snapshot

    fetcher = PolymarketFetcher()
    markets = fetcher.fetch_active_markets()

    inserted = 0
    skipped = 0

    with get_conn() as conn:
        for m in markets:
            raw_id = upsert_raw_market(conn, m)
            if raw_id is None:
                skipped += 1
                continue

            if m.probability is None:
                logger.warning(f"Skipping {m.external_id} - no valid probability")
                skipped += 1
                continue

            market_id = upsert_market(conn, raw_id, m, True)

            insert_snapshot(conn, market_id, m)
            inserted += 1

    logger.info(f"Polymarket ingestion: {inserted} inserted, {skipped} skipped")

    context["ti"].xcom_push(key="polymarket_inserted", value=inserted)
    return inserted

def run_kalshi_ingestion(**context):
    inserted=0
    logger.info("Kalshi ingestion: not yet implemented")
    context["ti"].xcom_push(key="kalshi_inserted", value=inserted)
    return inserted

def log_ingestion_summery(**context):
    ti = context["ti"]
    poly = ti.xcom_pull(task_ids="polymarket_ingestion", key="polymarket_inserted") or 0
    kalshi = ti.xcom_pull(task_ids="kalshi_ingestion", key="kalshi_inserted") or 0
    total = poly + kalshi
    logger.info(f"Ingestion summery - Polymarket: {poly}, Kalshi: {kalshi}, Total: {total}")
    return total


default_args = {
    "owner" : "pss",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": 'm.b.bachurin@gmail.com'
}

with DAG (
    dag_id = "pss_ingestion",
    default_args=default_args,
    description="Layer 1 - ingest active markets from Polymarket and Kalshi",
    schedule_interval=timedelta(minutes=15),
    catchup=False,
    tags=["pss", "ingestion"],
) as dag:
    polymarket_ingestion = PythonOperator(
        task_id="polymarket_ingestion",
        python_callable=run_polymarket_ingestion,
    )

    kalshi_ingestion = PythonOperator(
        task_id="kalshi_ingestion",
        python_callable=run_kalshi_ingestion,
    )

    ingestion_summery = PythonOperator(
        task_id="ingestion_summery",
        python_callable=log_ingestion_summery,
    )

    [polymarket_ingestion, kalshi_ingestion] >> ingestion_summery

