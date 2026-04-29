from abc import ABC, abstractmethod
from src.pss.datatypes.raw_market import RawMarket

class BaseFetcher(ABC):
    @abstractmethod
    def fetch_active_markets(self) -> list[RawMarket]:
        ...

