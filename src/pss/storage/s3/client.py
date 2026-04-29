import json
import boto3
from datetime import datetime, timezone
from src.pss.storage.shared.client import Client
from src.pss.ingestion.shared.base import RawMarket
from config.config import settings
from botocore.exceptions import ClientError
import logging

logger = logging.getLogger(__name__)

class S3Client(Client):
    def upload_markets(self, markets: list[RawMarket]) -> bool:
        if not markets:
            return False

        s3 = self._get_s3_client()

        sources = {m.source for m in markets}
        if len(sources) > 1:
            raise ValueError(f"Expected single-source batch, got: {sources}")
        source = sources.pop()

        key: str = f"raw/{source}/{datetime.now(timezone.utc).strftime('%Y/%m/%d/%H%M%S_%f')}.json"
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
            return status == 200
        except ClientError as e:
            logger.error(f"S3 upload failed for key {key}: {e}")
            return False

    # private
    @staticmethod
    def _get_s3_client():
        return boto3.client(
            "s3",
            aws_access_key_id=settings.aws_access_key,
            aws_secret_access_key=settings.aws_secret_access_key,
            region_name=settings.aws_region_name,
        )

    @staticmethod
    def _serialize(markets: list[RawMarket]) -> list[dict]:
        return [
            {
                "source": market.source,
                "external_id": market.external_id,
                "question": market.question,
                "probability": market.probability,
                "volume": market.volume,
                "category": market.category,
                "expiry": market.expiry.isoformat() if market.expiry else None,
                "raw_payload": market.raw_payload,
            }
            for market in markets
        ]