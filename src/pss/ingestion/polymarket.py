import logging
import time
from datetime import datetime, timezone, timedelta

import requests

from pss_config.config import settings
from src.pss.ingestion.shared.base import BaseFetcher
from src.pss.datatypes.raw_market import RawMarket
from src.pss.ingestion.shared.market_processing import  _parse_event

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
            "restricted": "false",
            "order" : "volume24hr",
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
                "restricted": "false",
                "tag_id" : tag_id,
                "order": "volume24hr",
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
        total_events_fetched = 0

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

            total_events_fetched += len(events)

            for event in events:
                all_markets.extend(_parse_event(event, logger))

            if len(events) < settings.polymarket_page_limit:
                break

            offset += settings.polymarket_page_limit
            time.sleep(0.2)

        logger.info(f"Total events fetched across all pages: {total_events_fetched}")
        return all_markets


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
