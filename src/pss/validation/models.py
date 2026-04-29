import logging
from datetime import datetime, timezone
from pydantic import BaseModel, field_validator, model_validator
from src.pss.ingestion.shared.base import RawMarket

logger = logging.getLogger(__name__)

VALID_SOURCES = {"polymarket", "kalshi"}

class ValidatedMarket(BaseModel):
    ...

def validate_market(market: RawMarket) -> ValidatedMarket | None:
    ...

def validate_markets(marekts: list[RawMarket]) -> list[ValidatedMarket]:
    ...