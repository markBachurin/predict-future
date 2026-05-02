from abc import ABC, abstractmethod
from src.pss.ingestion.shared.base import RawMarket

class Client(ABC):
    @abstractmethod
    def upload_markets(self, markets: list[RawMarket]) -> list[str]:
        ...