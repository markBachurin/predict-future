import json
import boto3
from datetime import datetime, timezone
from src.pss.storage.shared.client import Client
from src.pss.datatypes.raw_market import RawMarket
from src.pss.datatypes.validated_market import ValidatedMarket
from config.config import settings
from botocore.exceptions import ClientError
import logging
from typing import Union, Type

Market = Union[RawMarket, ValidatedMarket]
MarketClass = Type[Market]

logger = logging.getLogger(__name__)

class S3Client(Client):
    def upload_markets(self, markets: list[Market], prefix: str = "raw") -> str | None:
        if not markets:
            return None

        s3 = self._get_s3_client()

        sources = {m.source for m in markets}
        if len(sources) > 1:
            raise ValueError(f"Expected single-source batch, got: {sources}")
        source = sources.pop()

        key: str = f"{prefix}/{source}/{datetime.now(timezone.utc).strftime('%Y/%m/%d/%H%M%S_%f')}.json"
        body = json.dumps(self._serialize(markets), indent=2)

        try:
            response = s3.put_object(
                Bucket=settings.s3_bucket,
                Key=key,
                Body=body,
                ContentType="application/json"
            )
            status = response["ResponseMetadata"]["HTTPStatusCode"]
            logger.info(f"Archived {len(markets)} markets to s3://{settings.s3_bucket}/{key}")
            return key
        except ClientError as e:
            logger.error(f"S3 upload failed for key {key}: {e}")
            return None

    def download_raw_markets(self, key: str) -> list[RawMarket]:
        return self._download_markets(key, RawMarket)

    def download_validated_markets(self, key: str) -> list[ValidatedMarket]:
        return self._download_markets(key, ValidatedMarket)

    # private
    def _download_markets(self, key: str, market_class: MarketClass):
        if not key:
            return []
        try:
            s3 = self._get_s3_client()
            response = s3.get_object(Bucket=settings.s3_bucket, Key=key)
            data = json.loads(response["Body"].read().decode("utf-8"))
            return [market_class.from_dict(m) for m in data]
        except ClientError as e:
            logger.error(f"S3 download failed for key {key} : {e}")
            return []

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