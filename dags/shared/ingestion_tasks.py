from src.pss.storage.s3.client import S3Client
import logging
from src.pss.ingestion.shared.base import BaseFetcher


logger = logging.getLogger(__name__)

def task_fetch_and_archive(fetcher: BaseFetcher, **context):
    markets = fetcher.fetch_active_markets()
    key = S3Client().upload_markets(markets)

    logger.info(f"Fetched {len(markets)} markets, archived to {key}")
    context["ti"].xcom_push(key="s3_key", value=key)  # just a string — safe

def task_validate(**context):
    from src.pss.validation.models import validate_markets
    key = context["ti"].xcom_pull(key="s3_key")
    markets = S3Client().download_raw_markets(key)
    validated = validate_markets(markets)

    validated_key = S3Client().upload_markets(validated, prefix="validated")
    context["ti"].xcom_push(key="validated_s3_key", value=validated_key)

def task_load_postgres(**context):
    from src.pss.storage.postgres.client import PostgresClient
    validated_key = context["ti"].xcom_pull(key="validated_s3_key")

    validated = S3Client().download_validated_markets(validated_key)

    pg = PostgresClient()
    for m in validated:
        raw_ids = pg.upload_markets([m])
        if raw_ids:
            market_id = pg.upsert_market(raw_ids[0], m, is_valid=True)
            pg.insert_snapshot(market_id, m)