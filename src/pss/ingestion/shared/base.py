from abc import ABC, abstractmethod
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
    raw_payload : dict

class BaseFetcher(ABC):
    @abstractmethod
    def fetch_active_markets(selfsef) -> list[RawMarket]:
        ...

