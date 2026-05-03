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

    def _parse_event(self, event: dict) -> list[RawMarket]:
        results = []
        category = self._extract_category(event)

        for market in event.get("markets", []):
            volume = float(event.get("volume") or market.get("volumeNum") or 0)
            if volume < settings.polymarket_volume_min:
                continue

            description = market.get("description") or event.get("description")

            volume24hr = float(market.get("volume24hr") or 0)

            price_change_day = float(market.get("oneDayPriceChange")) if market.get("oneDayPriceChange") is not None else None
            price_change_week = float(market.get("oneWeekPriceChange")) if market.get("oneWeekPriceChange") is not None else None

            liquidity = float(market.get("liquidity") or 0)
            if liquidity < settings.polymarket_liquidity_min:
                continue

            tags=[t.get("label", "") for t in event.get("tags", [])]

            event_ticker = event.get("ticker")
            market_type = market.get("market_type")


            outcomes_raw = market.get("outcomes", [])
            if isinstance(outcomes_raw, str):
                try:
                    outcomes_list = json.loads(outcomes_raw)
                except (json.JSONDecodeError, TypeError):
                    outcomes_list = []
            else:
                outcomes_list = outcomes_raw if outcomes_raw is not None else []

            probs_raw = market.get("probabilities", [])
            if isinstance(probs_raw, str):
                try:
                    probs_list = json.loads(probs_raw)
                except (json.JSONDecodeError, TypeError):
                    probs_list = []
            else:
                probs_list = probs_raw if probs_raw is not None else []


            outcome_probabilities_list = []
            for p in probs_list:
                try:
                    if p is not None:
                        outcome_probabilities_list.append(float(p))
                except (ValueError, TypeError):
                    continue

            resolution_source = market.get("resolution_source")
            restricted_status = market.get("restricted", False)

            prob=None                   # for a non-binary markets, prob remains None, indicating that a single probability is not directly applicable!!!!!!
            if market_type == "Binary" and "Yes" in outcomes_list:
                try:
                    yes_index = outcomes_list.index("Yes")
                    if 0 <= yes_index <= len(outcome_probabilities_list):
                        prob = outcome_probabilities_list[yes_index]
                        if not (0.0 <= prob <= 1.0):
                            logger.warning(f"Binary 'Yes' probability out of range(0.0 - 1.0) for market {market.get('id')} : {prob}")
                    else:
                        logger.warning(f"Binary 'Yes' outcome index out of bounds for market {market.get('id')}. Setting prob to None.")
                except ValueError:
                    logger.warning(f"Could not find 'Yes' outcome for binary market {market.get('id')}. Setting prob to None.")
                except IndexError:
                    logger.warning(f"Outcome/probability list length mismatch for binary market {market.get('id')}. Setting prob to None.")


            results.append(RawMarket(
                source="polymarket",
                external_id=f"polymarket:{market.get('conditionId', market.get('id'))}",
                question=market.get("question", event.get("title", "")),
                description=description,
                probability=prob,
                volume=volume,
                category=category,
                expiry=self._parse_expiry(event.get("endDate")),
                volume24hr=volume24hr,
                price_change_day=price_change_day,
                price_change_week=price_change_week,
                liquidity=liquidity,
                tags=tags,
                market_type=market_type,
                outcomes=outcomes_list,
                outcome_probabilities=outcome_probabilities_list,
                resolution_source=resolution_source,
                ticker=event_ticker,
                restricted=restricted_status,

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
