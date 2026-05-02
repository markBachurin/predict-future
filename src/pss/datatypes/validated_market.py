import logging
from datetime import datetime, timezone
from pydantic import BaseModel, field_validator, model_validator

logger = logging.getLogger(__name__)

VALID_SOURCES = {"polymarket", "kalshi"}

class ValidatedMarket(BaseModel):
    source: str
    external_id: str
    question: str
    description: str | None
    probability: float | None
    volume: float
    category: str | None
    expiry: datetime | None
    volume24hr: float
    price_change_day: float | None
    price_change_week: float | None

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

    @field_validator("volume24hr")
    @classmethod
    def volume24hr_non_negative(cls, v:float) -> float:
        if v < 0:
            raise ValueError(f"volume24hr must be non negative, got: {v}")
        return v

    @field_validator("price_change_day")
    @classmethod
    def price_change_day_non_negative(cls, v: float | None) -> float | None:
        return v

    @field_validator("price_change_week")
    @classmethod
    def price_change_week_non_negative(cls, v: float | None) -> float | None:
        return v

    def to_dict(self) -> dict:
        return {
            "source": self.source,
            "external_id": self.external_id,
            "question": self.question,
            "description": self.description,
            "probability": self.probability,
            "volume": self.volume,
            "category": self.category,
            "expiry": self.expiry.isoformat() if self.expiry else None,
            "volume24hr" : self.volume24hr,
            "price_change_day": self.price_change_day,
            "price_change_week": self.price_change_week,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "ValidatedMarket":
        return cls(
            source=data["source"],
            external_id=data["external_id"],
            question=data["question"],
            description=data["description"],
            probability=data["probability"],
            volume=data["volume"],
            category=data["category"],
            expiry=datetime.fromisoformat(data["expiry"]) if data["expiry"] else None,
            volume24hr=data["volume24hr"],
            price_change_day=data["price_change_day"],
            price_change_week=data["price_change_week"],
        )
