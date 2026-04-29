import logging
from datetime import datetime, timezone
from pydantic import BaseModel, field_validator, model_validator
from src.pss.ingestion.shared.base import RawMarket

logger = logging.getLogger(__name__)

VALID_SOURCES = {"polymarket", "kalshi"}

class ValidatedMarket(BaseModel):
    source: str
    external_id: str
    question: str
    probability: float | None
    volume: float
    category: str | None
    expiry: datetime | None
    raw_payload: dict

    @field_validator("source")
    @classmethod
    def source_must_be_known(cls, v: str) -> str:
        if v not in VALID_SOURCES:
            raise ValueError(f"Unknown source: '{v}'. Must be one of {VALID_SOURCES}")
        return v

    @field_validator("external_id")
    @classmethod
    def external_id_not_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("external_id must not be empty")
        return v

    @field_validator("question")
    @classmethod
    def question_not_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("question must not be empty")
        return v

    @field_validator("probability")
    @classmethod
    def probability_in_range(cls, v: float | None) -> float:
        if v is None:
            raise ValueError(f"Probability is required")
        if not (0.0 <= v <= 1.0):
            raise ValueError(f"Probability must be between 0.0 and 1.0, got {v}")
        return v

    @field_validator("volume")
    @classmethod
    def volume_non_negative(cls, v: float) -> float:
        if v < 0:
            raise ValueError(f"Volume must be non negative, got: {v}")
        return v

    @model_validator(mode="after")
    def expiry_not_in_past(self) -> "ValidatedMarket":
        if self.expiry and self.expiry < datetime.now(timezone.utc):
            raise ValueError(f"Market already expired at {self.expiry.isoformat()}")
        return self


def validate_market(market: RawMarket) -> ValidatedMarket | None:
    try:
        return ValidatedMarket(
            source = market.source,
            external_id= market.external_id,
            question= market.question,
            probability= market.probability,
            volume= market.volume,
            category= market.category,
            expiry= market.expiry,
            raw_payload= market.raw_payload,

        )
    except Exception as e:
        logger.warning(f"Dropping invalid market  [{market.source}:{market.external_id}] - {e}")
        return None


def validate_markets(markets: list[RawMarket]) -> list[ValidatedMarket]:
    results = []
    dropped = 0

    for market in markets:
        validated = validate_market(market)
        if validated:
            results.append(validated)
        else:
            dropped += 1

    logger.info(
        f"Validation complete - {len(results)} valid, {dropped} dropped out of {len(markets)} total"
    )
    return results