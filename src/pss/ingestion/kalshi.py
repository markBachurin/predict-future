import logging
import time
from datetime import datetime, timezone, timedelta

import requests

from pss_config.config import settings
from src.pss.ingestion.shared.base import BaseFetcher
from src.pss.datatypes.raw_market import RawMarket

logger = logging.getLogger(__name__)

SIGNAL_SERIES = [
    # Crypto
    "KXBTC", "KXBTCMAX100", "KXBTCMAX150",
    "KXETH", "KXBNB", "KXBNBD", "KXSOLD",
    "KXHYPE", "KXDOGE",
    # Fed / macro
    "KXFEDDECISION", "KXFED", "KXRATECUT", "KXRATECUTCOUNT",
    "KXFEDHIKE", "KXDOTPLOT", "KXFOMCDISSENTCOUNT", "KXFEDMEET",
    "KXZERORATE",
    # Inflation
    "KXCPI", "KXCPIYOY", "KXCPICORE", "KXCPICOREYOY",
    # Indices
    "KXINX", "KXINXY", "KXNASDAQ100", "KXNASDAQ100Y",
    # Yield / treasuries
    "KX10Y2Y", "KX10Y3M",
    # AI / semiconductors
    "KXH200W", "KXH200MON", "KXB200W", "KXB200MON",
    "KXA100W", "KXA100MON", "KXRTX5090W", "KXRTX5090MON",
    # AI general
    "KXTOPAI", "KXOAIAGI", "KXFRONTIER",
    # GDP / employment
    "KXGDP", "KXGDPYEAR", "KXPAYROLLS", "KXUSNFP", "KXJOBLESSCLAIMS",
]

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
        seen = set()

        for series_ticker in SIGNAL_SERIES:
            cursor = None
            while True:
                params = {
                    "status": "open",
                    "limit": 1000,
                    "series_ticker": series_ticker,
                }
                if cursor:
                    params["cursor"] = cursor

                try:
                    resp = self.session.get(
                        settings.kalshi_base_url + "/markets",
                        params=params,
                        timeout=30,
                    )
                    if resp.status_code == 429:
                        retry_after = resp.headers.get("Retry-After")
                        rate_limit = resp.headers.get("X-RateLimit-Limit")
                        remaining = resp.headers.get("X-RateLimit-Remaining")
                        reset = resp.headers.get("X-RateLimit-Reset")
                        logger.warning(
                            f"Rate limited on series={series_ticker} | "
                            f"Retry-After={retry_after} | "
                            f"Limit={rate_limit} | Remaining={remaining} | Reset={reset} | "
                            f"retrying in 2s..."
                        )
                        time.sleep(2)
                        continue
                    resp.raise_for_status()
                    data = resp.json()
                except requests.HTTPError as e:
                    logger.error(f"Kalshi HTTP error (series={series_ticker}): {e}")
                    break
                except Exception as e:
                    logger.error(f"Kalshi fetch error (series={series_ticker}): {e}")
                    break

                markets = data.get("markets", [])
                if not markets:
                    break

                for market in markets:
                    ticker = market.get("ticker")
                    if ticker in seen:
                        continue
                    seen.add(ticker)
                    parsed = self._parse_market(market)
                    if parsed:
                        all_markets.append(parsed)

                cursor = data.get("cursor")
                if not cursor:
                    break

                time.sleep(1)

        logger.info(f"Kalshi: {len(all_markets)} markets fetched across {len(SIGNAL_SERIES)} series")
        return all_markets

    def _parse_market(self, market: dict) -> RawMarket | None:
        expiry = self._parse_expiry(market.get("close_time"))
        if expiry and expiry > self.expiry_max:
            return None

        prob = self._parse_probability(market)
        if prob is None:
            return None

        volume = self._parse_volume(market)
        if volume < settings.min_volume:
            return None

        return RawMarket(
            source="kalshi",
            external_id=f"kalshi:{market.get('ticker')}",
            question=market.get("title", ""),
            probability=prob,
            volume=self._parse_volume(market),
            category=market.get("category", ""),
            expiry=expiry,
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