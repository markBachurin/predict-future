import logging
import asyncio
from src.pss.llm.client import LLMClient
from src.pss.storage.postgres.client import PostgresClient
from pss_config.config import settings
import math
from decimal import Decimal

logger = logging.getLogger(__name__)


class MarketClassifier:
    def __init__(self, llm_client: LLMClient, pg_client: PostgresClient, batch_size: int = 10):
        self.llm = llm_client
        self.pg = pg_client
        self.semaphore1 = asyncio.Semaphore(settings.gatekeep_thread_limit)
        self.semaphore2 = asyncio.Semaphore(settings.reason_thread_limit)
        self.batch_size = batch_size

    async def classify_all(self):
        markets = self.pg.get_markets_for_classification()
        if not markets:
            logger.info("No new markets to classify.")
            return

        logger.info(f"Starting to classify {len(markets)} markets")
        all_raw_market_ids = [m["raw_market_id"] for m in markets]

        # Pass 1 — Question filter
        # TODO: batch markets by batch_size, send question + tags only
        # filter down to relevant_markets based on response

        # Pass 2 — Description reasoning
        # TODO: no batching, one market at a time
        # send question + description + liquidity + probability + price_change
        # build classifications list

        # Insert and mark processed
        # TODO: insert classifications, mark all_raw_market_ids as processed

    # private

    async def _question_batch(self, batch: list[dict]) -> list[dict]:
        async with self.semaphore1:
            # TODO: system prompt focused on question + tags relevance filtering
            # prompt contains only market_id, question, tags per market
            # returns JSON array with market_id, is_relevant, confidence, reason
            pass

    async def _description_pass(self, market: dict) -> dict:
        async with self.semaphore2:
            # TODO: system prompt focused on deep analysis
            # prompt contains question + description + liquidity + probability + price_change
            # no batching — one market at a time
            # returns single JSON object with full classification fields
            pass

    @staticmethod
    def _chunk_markets(markets: list[dict], batch_size: int):
        for i in range(0, len(markets), batch_size):
            yield markets[i:i + batch_size]

    @staticmethod
    def _calculate_weighted_score(market: dict, analysis: dict) -> float:
        llm_conf = Decimal(str(analysis.get("llm_confidence", 0.0)))
        vol = max(market.get("volume", 0.0), settings.polymarket_volume_min)

        log_vol = math.log(float(vol))
        normal_vol_float = (log_vol - 0.0) / (16.1 - 10.8)
        normal_vol = Decimal(str(max(0, min(1.0, normal_vol_float))))

        price_change = abs(Decimal(market.get("price_change_day") or 0.0))
        norm_price = min(price_change / Decimal('0.1'), Decimal('1.0'))

        score = (llm_conf * Decimal('0.4')) + (normal_vol * Decimal('0.4')) + (norm_price * Decimal('0.2'))
        return round(float(score), 4)

    @staticmethod
    def _get_question_prompt(market: dict) -> str:
        # Pass 1 — minimal, question + tags only
        return (
            f"MARKET_ID: {market['market_id']}\n"
            f"Question: {market['question']}\n"
            f"Tags: {market.get('tags', [])}\n"
        )

    @staticmethod
    def _get_description_prompt(market: dict) -> str:
        # Pass 2 — focused on signal strength and analysis
        return (
            f"Question: {market['question']}\n"
            f"Description: {market.get('description', '')}\n"
            f"Probability: {market.get('probability')}\n"
            f"Liquidity: {market.get('liquidity', 0.0)}\n"
            f"Price change 24h: {market.get('price_change_day', 0.0)}\n"
            f"Price change week: {market.get('price_change_week', 0.0)}\n"
        )