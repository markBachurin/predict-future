from dataclasses import dataclass
from datetime import datetime

@dataclass
class RawMarket:
    source: str
    external_id: str
    question: str
    probability: float | None
    volume : float
    category : str | None
    expiry : datetime | None

    def to_dict(self) -> dict:
        return {
            "source": self.source,
            "external_id": self.external_id,
            "question": self.question,
            "probability": self.probability,
            "volume": self.volume,
            "category": self.category,
            "expiry": self.expiry.isoformat() if self.expiry else None,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "RawMarket":
        return cls(
            source=data["source"],
            external_id=data["external_id"],
            question=data["question"],
            probability=data["probability"],
            volume=data["volume"],
            category=data["category"],
            expiry=datetime.fromisoformat(data["expiry"]) if data["expiry"] else None,
        )