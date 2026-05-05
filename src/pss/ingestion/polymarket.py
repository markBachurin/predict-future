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

EXCLUDED_CATEGORIES = {
    "sports", "esports", "soccer", "nhl", "weather", "culture", "movies", 
    "eurovision", "tennis", "music", "madrid open", "awards", "formula 1", 
    "fifa world cup", "champions league", "games", "nba", "cristiano ronaldo", 
    "reality tv", "netflix", "anime", "malta", "football", "league of legends", 
    "celebrities", "pop culture", "entertainment", "boxing", "ufc", "horse racing",
    "cricket", "golf", "baseball", "rugby", "hockey"
}

NOISE_KEYWORDS = {
    "youtube", "tiktok", "instagram", "follower", "subscriber", "dating", 
    "marriage", "divorce", "oscar", "grammy", "emmy", "mrbeast", "streamer",
    "box office", "trailer", "rotten tomatoes", "metacritic", "album", 
    "concert", "tour", "leak", "spoiler", "death", "hospitalized"
}

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

    def _should_skip_event(self, category: str | None, tags: list[str], title: str) -> bool:
        category_lower = category.lower() if category else ""
        tags_lower = [t.lower() for t in tags]

        if category_lower in EXCLUDED_CATEGORIES or any(t in EXCLUDED_CATEGORIES for t in tags_lower):
            logger.debug(f"Skipping irrelevant category/tag: {category} | {tags}")
            return True

        if any(kw in title for kw in NOISE_KEYWORDS):
            logger.debug(f"Skipping event due to noise keyword: {title}")
            return True
        
        return False

    def _parse_event(self, event: dict) -> list[RawMarket]:
        results = []
        category = self._extract_category(event)
        tags = [t.get("label", "") for t in event.get("tags", [])]
        title = event.get("title", "").lower()

        if self._should_skip_event(category, tags, title):
            return []

        parsed_markets = []
        for market in event.get("markets", []):
            volume = float(event.get("volume") or market.get("volumeNum") or 0)
            if volume < settings.polymarket_volume_min:
                continue

            liquidity = float(market.get("liquidity") or 0)
            if liquidity < settings.polymarket_liquidity_min:
                continue

            market_type = market.get("market_type") or market.get("marketType")
            outcomes_raw = market.get("outcomes", [])
            outcomes_list = json.loads(outcomes_raw) if isinstance(outcomes_raw, str) else (outcomes_raw or [])

            if not market_type:
                market_type = "Binary" if len(outcomes_list) == 2 else "Multi-Outcome"

            probs_raw = market.get("outcomePrices") or market.get("probabilities") or []
            probs_list = json.loads(probs_raw) if isinstance(probs_raw, str) else (probs_raw or [])
            outcome_probabilities_list = []
            for p in probs_list:
                try:
                    if p is not None:
                        outcome_probabilities_list.append(float(p))
                except (ValueError, TypeError):
                    continue

            # extract single probability for Binary markets
            prob = None
            if market_type == "Binary" and len(outcomes_list) == 2 and len(outcome_probabilities_list) == 2:
                yes_index = outcomes_list.index("Yes") if "Yes" in outcomes_list else 0
                prob = outcome_probabilities_list[yes_index]

            if prob is not None and not (0.0 <= prob <= 1.0):
                prob = None

            parsed_markets.append({
                "market": market,
                "market_type": market_type,
                "outcomes": outcomes_list,
                "probs": outcome_probabilities_list,
                "prob": prob,
                "volume": volume,
                "liquidity": liquidity
            })

        if not parsed_markets:
            return []

        # clustered Event Deduplication, center-of-gravity, select the single market closest to 0.5 probability, if no binary probability, fall back to highest volume
        best_market_data = None
        min_distance = 1.1  # max distance is 0.5

        for m_data in parsed_markets:
            p = m_data["prob"]
            if p is not None:
                # market has a binary probability
                distance = abs(p - 0.5)
                if best_market_data is None or best_market_data["prob"] is None or distance < min_distance:
                    min_distance = distance
                    best_market_data = m_data
            else:
                # no binary probability (multi-outcome or missing price)
                if best_market_data is None:
                    best_market_data = m_data
                elif best_market_data["prob"] is None and m_data["volume"] > best_market_data["volume"]:
                    # both have no prob, pick the one with more volume
                    best_market_data = m_data

        if not best_market_data:
            return []

        m = best_market_data["market"]
        results.append(RawMarket(
            source="polymarket",
            external_id=f"polymarket:{m.get('conditionId', m.get('id'))}",
            question=m.get("question", event.get("title", "")),
            description=m.get("description") or event.get("description"),
            probability=best_market_data["prob"],
            volume=best_market_data["volume"],
            category=category,
            expiry=self._parse_expiry(event.get("endDate")),
            volume24hr=float(m.get("volume24hr") or 0),
            price_change_day=float(m.get("oneDayPriceChange")) if m.get("oneDayPriceChange") is not None else None,
            price_change_week=float(m.get("oneWeekPriceChange")) if m.get("oneWeekPriceChange") is not None else None,
            liquidity=best_market_data["liquidity"],
            tags=tags,
            market_type=best_market_data["market_type"],
            outcomes=best_market_data["outcomes"],
            outcome_probabilities=best_market_data["probs"],
            resolution_source=m.get("resolution_source") or m.get("resolutionSource"),
            ticker=event.get("ticker"),
            restricted=m.get("restricted", False),
        ))

        return results


    @staticmethod
    def _parse_expiry(end_date_str: str | None) -> datetime | None:
        if not end_date_str:
            return None
        try:
            return datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
        except ValueError as e:
            logger.error(f"_parse_expiry error : {e}")
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
