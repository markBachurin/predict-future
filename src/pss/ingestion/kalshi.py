import logging
import time
from datetime import datetime, timezone, timedelta

import requests

from config.config import settings
from src.pss.ingestion.shared.base import BaseFetcher
from src.pss.datatypes.raw_market import RawMarket

logger = logging.getLogger(__name__)

SIGNAL_CATEGORIES = {"economics", "finance", "crypto", "politics", "technology", "business"}

class KalshiFetcher(BaseFetcher):
    def __init__(self):
        self.session = requests.Session()
        self.expiry_max =  datetime.now(timezone.utc) + timedelta(days=settings.expiry_max_days)

    def fetch_active_markets(self) -> list[RawMarket]:
        markets : list[RawMarket] = self._fetch_open_markets()
        logger.info(f"Kalshi: {len(markets)} markets fetched")
        return markets

    # private
    def _fetch_open_markets(self) -> list[RawMarket]:
        all_markets = []
        cursor = None

        while True:
            params = {
                "status": "open",
                "limit": 1000,
            }
            if cursor:
                params["cursor"] = cursor
            try:
                resp = self.session.get(
                    settings.kalshi_base_url + "/markets",
                    params=params,
                    timeout=30,
                )
                resp.raise_for_status()
                data = resp.json()
            except requests.HTTPError as e:
                logger.error(f"Kalshi HTTP error: {e}")
                break
            except Exception as e:
                logger.error(f"Kalshi fetch error: {e}")
                break

            markets = data.get("markets", [])
            if not markets:
                break

            for market in markets:
                parsed = self._parse_market(market)
                if parsed:
                    all_markets.append(parsed)

            cursor = data.get("cursor")
            if not cursor:
                break

            time.sleep(0.2)

        return all_markets

    def _parse_market(self, market: dict) -> RawMarket | None:
        category = market.get("category", "")
        if category.lower() not in SIGNAL_CATEGORIES:
            return None

        expiry = self._parse_expiry(market.get("close_time"))
        if expiry and expiry > self.expiry_max:
            return None

        prob = self._parse_probability(market)
        if prob is None:
            return None

        return RawMarket(
            source="kalshi",
            external_id=f"kalshi:{market.get('ticker')}",
            question=market.get("title", ""),
            probability=prob,
            volume=self._parse_volume(market),
            category=category,
            expiry=expiry,
            raw_payload=market,
        )

    @staticmethod
    def _parse_probability(market: dict) -> float| None:
        try:
            bid = market.get("yes_bid_dollars")
            ask = market.get("yes_ask_dollars")

            if bid is not None and ask is not None:
                prob = (float(bid) + float(ask)) / 2
            elif bid is not None:
                prob = float(bid)
            elif ask is not None:
                prob = float(ask)
            else:
                return None

            return prob if 0.0 <= prob <= 1.0 else None
        except (ValueError, TypeError) as e:
            logger.error(f"Parse Probability error: {e}")
            return None

    @staticmethod
    def _parse_volume(market: dict) -> float:
        try:
            return float(market.get("volume_fp") or market.get("volume") or 0)
        except (ValueError, TypeError) as e:
            logger.error(f"Parsing Volume error: {e}")
            return 0.0

    @staticmethod
    def _parse_expiry(close_time: str | None) -> datetime | None:
        if not close_time:
            return None
        try:
            return datetime.fromisoformat(close_time.replace("Z", "+00:00"))
        except ValueError as e:
            logger.error(f"Parse Expiry error: {e}")
            return None