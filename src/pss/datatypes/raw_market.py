from dataclasses import dataclass
from datetime import datetime

@dataclass
class RawMarket:
    source: str
    external_id: str
    question: str
    description:str | None
    probability: float | None
    volume : float
    category : str | None
    expiry : datetime | None
    volume24hr: float
    price_change_day: float | None
    price_change_week: float | None
    liquidity: float
    tags: list[str]
    market_type: str | None
    outcomes: list[str]
    outcome_probabilities: list[float]
    resolution_source: str | None
    ticker: str | None
    restricted: bool = False

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
            "liquidity": self.liquidity,
            "tags": self.tags,
            "market_type": self.market_type,
            "outcomes": self.outcomes,
            "outcome_probabilities": self.outcome_probabilities,
            "resolution_source": self.resolution_source,
            "ticker": self.ticker,
            "restricted": self.restricted,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "RawMarket":
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
            liquidity=data["liquidity"],
            tags=data["tags"],
            market_type=data.get("market_type"),
            outcomes=data.get("outcomes", []),
            outcome_probabilities=data.get("outcome_probabilities", []),
            resolution_source=data.get("resolution_source"),
            ticker=data.get("ticker"),
            restricted=data.get("restricted", False),
        )