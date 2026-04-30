import json
import logging
import time
from datetime import datetime, timezone, timedelta

import requests

from pss_config.config import settings
from src.pss.ingestion.shared.base import BaseFetcher
from src.pss.datatypes.raw_market import RawMarket

logger = logging.getLogger(__name__)

SIGNAL_KEYWORDS = {"economics", "finance", "crypto", "politics", "technology", "business"}

class PolymarketFetcher(BaseFetcher):
    def __init__(self):
        self.session = requests.Session()
        self._tag_ids: list[str] | None = None

    def fetch_active_markets(self) -> list[RawMarket]:
        broad = self._fetch_broad()
        targeted = self._fetch_targeted()

        # deduplicate by external_id
        seen: dict[str, RawMarket] = {}
        for m in broad + targeted:
            seen[m.external_id] = m

        logger.info(f"Polymarket: {len(seen)} unique markets fetched")
        return list(seen.values())


    # private

    def _fetch_broad(self) -> list[RawMarket]:
        now = datetime.now(timezone.utc)
        expiry_max = now + timedelta(days=settings.expiry_max_days)
        params = {
            "active" : "true",
            "closed": "false",
            "archived": "false",
            "volume_min": settings.polymarket_volume_min,
            "order" : "volume_24hr",
            "end_date_min" : now.isoformat(),
            "end_date_max": expiry_max.isoformat(),
            "limit": settings.polymarket_page_limit,
        }

        markets = self._paginate("/events", params)
        logger.info(f"Broad sweep: {len(markets)} markets")
        return markets

    def _fetch_targeted(self) -> list[RawMarket]:
        tag_ids = self._get_signal_tag_ids()
        now = datetime.now(timezone.utc)
        results = []

        for tag_id in tag_ids:
            params = {
                "active": "true",
                "closed": "false",
                "archived": "false",
                "tag_id" : tag_id,
                "volume_min": 5_000,
                "order": "volume_24hr",
                "ascending":"false",
                "end_date_min": now.isoformat(),
                "limit": settings.polymarket_page_limit,
            }
            markets = self._paginate("/events", params)
            logger.info(f"Tag {tag_id}: {len(markets)} markets")
            results.extend(markets)
        return results

    def _paginate(self, endpoint: str, params: dict) -> list[RawMarket]:
        all_markets = []
        offset = 0

        while True:
            params["offset"] = offset

            try:
                resp = self.session.get(settings.polymarket_base_url + endpoint, params=params, timeout=30)
                resp.raise_for_status()
                events = resp.json()
            except requests.HTTPError as e:
                logger.error(f"HTTP error: {e}")
                break
            except Exception as e:
                logger.error(f"Fetch error: {e}")
                break

            if not events:
                break

            for event in events:
                all_markets.extend(self._parse_event(event))

            if len(events) < settings.polymarket_page_limit:
                break

            offset += settings.polymarket_page_limit
            time.sleep(0.2)

        return all_markets

    def _parse_event(self, event: dict) -> list[RawMarket]:
        results = []
        category = self._extract_category(event)

        for market in event.get("markets", []):
            prob = self._parse_probability(market.get("outcomePrices"))
            if prob is None:
                continue

            results.append(RawMarket(
                source="polymarket",
                external_id=f"polymarket:{market.get('conditionId', market.get('id'))}",
                question=market.get("question", event.get("title", "")),
                probability=prob,
                volume=float(event.get("liquidity") or market.get("volumeNum") or 0),
                category=category,
                expiry=self._parse_expiry(event.get("endDate")),
            ))

        return results

    @staticmethod
    def _parse_probability(outcome_prices_raw) -> float | None:
        if not outcome_prices_raw:
            return None
        try:
            prices = json.loads(outcome_prices_raw) if isinstance(outcome_prices_raw, str) else outcome_prices_raw
            prob = float(prices[0])
            return prob if 0.0 <= prob <= 1.0 else None
        except (ValueError, IndexError, TypeError) as e:
            logger.debug(f"Could not parse outcomePrices: {outcome_prices_raw} — {e}")
            return None

    @staticmethod
    def _parse_expiry(end_date_str: str | None) -> datetime | None:
        if not end_date_str:
            return None
        try:
            return datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
        except ValueError:
            return None

    def _get_signal_tag_ids(self) -> list[str]:
        if self._tag_ids is not None:
            return self._tag_ids
        try:
            resp = self.session.get(settings.polymarket_base_url + "/tags", timeout=30)
            resp.raise_for_status()
            tags = resp.json()
            self._tag_ids = [
                str(t["id"]) for t in tags
                if any(kw in t.get("label", "").lower() for kw in SIGNAL_KEYWORDS)
            ]
            logger.info(f"Signal tag IDs: {self._tag_ids}")
            return self._tag_ids
        except Exception as e:
            logger.warning(f"Could not fetch tags: {e}")
            self._tag_ids = []
            return []

    @staticmethod
    def _extract_category(event: dict) -> str | None:
        tags = event.get("tags", [])
        return tags[0].get("label") if tags else None
