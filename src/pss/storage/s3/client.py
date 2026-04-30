import json
import boto3
from datetime import datetime, timezone
from src.pss.storage.shared.client import Client
from src.pss.datatypes.raw_market import RawMarket
from src.pss.datatypes.validated_market import ValidatedMarket
from pss_config.config import settings
from botocore.exceptions import ClientError
import logging
from typing import Union, Type

Market = Union[RawMarket, ValidatedMarket]
MarketClass = Type[Market]

logger = logging.getLogger(__name__)

class S3Client(Client):
    def upload_markets(self, markets: list[Market], prefix: str = "raw") -> list[str]:
        if not markets:
            return []

        s3 = self._get_s3_client()

        sources = {m.source for m in markets}
        if len(sources) > 1:
            raise ValueError(f"Expected single-source batch, got: {sources}")
        source = sources.pop()

        BATCH_SIZE = settings.batch_size
        date_path = datetime.utcnow().strftime("%Y:%m:%d")
        timestamp = datetime.utcnow().strftime("%H%M%S_%f")
        keys = []

        batches = [markets[i:i + BATCH_SIZE] for i in range(0, len(markets), BATCH_SIZE)]

        for idx, batch in enumerate(batches):
            key = f"{prefix}/{source}/{date_path}/{timestamp}/batch{idx}.json"
            try:
                s3.put_object(
                    Bucket=settings.s3_bucket,
                    Key=key,
                    Body=json.dumps([m.to_dict() for m in batch]),
                    ContentType="application/json",
                )
                keys.append(key)
                logger.info(f"Uploaded batch {idx+1}/{len(batches)} to {key}")
            except Exception as e:
                logger.error(f"S3 upload failed for key {key}: {e}")
                raise

        return keys

    def download_raw_markets(self, keys: list[str]) -> list[RawMarket]:
        return self._download_markets(keys, RawMarket)

    def download_validated_markets(self, keys: list[str]) -> list[ValidatedMarket]:
        return self._download_markets(keys, ValidatedMarket)

    # private
    def _download_markets(self, keys: list[str], market_class: MarketClass) -> list[Market]:
        if not keys:
            return []

        markets = []
        for key in keys:
            try:
                s3 = self._get_s3_client()
                response = s3.get_object(Bucket=settings.s3_bucket, Key=key)
                data = json.loads(response["Body"].read().decode("utf-8"))
                markets.extend([market_class.from_dict(m) for m in data])
            except ClientError as e:
                logger.error(f"S3 download failed for key {key} : {e}")
                raise
        return markets

    @staticmethod
    def _get_s3_client():
        return boto3.client(
            "s3",
            aws_access_key_id=settings.aws_access_key,
            aws_secret_access_key=settings.aws_secret_access_key,
            region_name=settings.aws_region_name,
        )

    @staticmethod
    def _serialize(markets: list[Market]) -> list[dict]:
        return [
            market.to_dict()
            for market in markets
        ]