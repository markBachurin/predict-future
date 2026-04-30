from src.pss.storage.s3.client import S3Client
import logging
from src.pss.ingestion.shared.base import BaseFetcher
from src.pss.storage.postgres.db_init import db_init


logger = logging.getLogger(__name__)

def task_fetch_and_archive(fetcher: BaseFetcher, **context):
    markets = fetcher.fetch_active_markets()
    keys: list[str] = S3Client().upload_markets(markets)

    logger.info(f"Fetched {len(markets)} markets, archived to {len(keys)} batches")
    context["ti"].xcom_push(key="s3_key", value=keys)  # just a string — safe

def task_validate(**context):
    from src.pss.validation.models import validate_markets
    keys = context["ti"].xcom_pull(key="s3_key")
    markets = S3Client().download_raw_markets(keys)
    validated = validate_markets(markets)

    validated_keys = S3Client().upload_markets(validated, prefix="validated")
    context["ti"].xcom_push(key="validated_s3_keys", value=validated_keys)

def task_load_postgres(**context):
    from src.pss.storage.postgres.client import PostgresClient
    validated_keys = context["ti"].xcom_pull(key="validated_s3_keys")

    validated = S3Client().download_validated_markets(validated_keys)

    pg = PostgresClient()
    raw_ids = pg.upload_markets(validated)
    if raw_ids:
        market_ids = pg.upsert_markets(raw_ids, validated, True)
        pg.insert_snapshots(market_ids, validated)
    else:
        logger.error(f"Error upserting markets")
        raise


def ensure_schema():
    return db_init()