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

        # Pass 1 - Question filter
        market_batches = list(self._chunk_markets(markets, self.batch_size))
        question_filter_batch_tasks = [self._question_batch(batch) for batch in market_batches]
        question_filter_raw_results = await asyncio.gather(*question_filter_batch_tasks)

        question_filter_results_map = {}
        for batch_results in question_filter_raw_results:
            if isinstance(batch_results, list):
                for res in batch_results:
                    if "market_id" in res:
                        question_filter_results_map[res["market_id"]] = res
                    else:
                        logger.warning(f"Question filter batch result missing 'market_id': {res}")
            else:
                logger.error(f"Unexpected question filter batch result format: {batch_results}")

        relevant_markets = []
        for market in markets:
            market_id = market["market_id"]
            result = question_filter_results_map.get(market_id, {'is_relevant': False, "confidence": 0.0})
            if result.get("is_relevant") and result.get("confidence", 0) > 0.7:
                market["question_filter_confidence"] = result.get("confidence")
                relevant_markets.append(market)

        logger.info(f"Question filter: {len(markets)} -> {len(relevant_markets)} relevant markets (confidence > 0.7).")

        if not relevant_markets:
            logger.info("No relevant markets after question filter. Marking all initial markets as processed and returning.")
            self.pg.mark_processed(all_raw_market_ids)
            return

        for id in question_filter_results_map:
            print(id, " ", question_filter_results_map[id])

        # Pass 2 - Description reasoning
        # TODO: no batching, one market at a time
        # send question + description + liquidity + probability + price_change
        # build classifications list

        # Insert and mark processed
        # TODO: insert classifications, mark all_raw_market_ids as processed

    # private

    async def _question_batch(self, batch: list[dict]) -> list[dict]:
        async with self.semaphore1:
            system_prompt = (
                "You are an aggressive gatekeeper for BIT Capital. "
                "Your ONLY job is to determine if a market is RELEVANT to BIT Capital's holdings "
                "based SOLELY on its QUESTION and TAGS. Be extremely strict and ruthlessly filter. "
                "When in doubt, mark NOT relevant. False negatives are acceptable. False positives waste resources.\n\n"
                "A market is RELEVANT only if its question or tags DIRECTLY name ONE of the following:\n"
                "1. A ticker from BIT_CAPITAL_HOLDINGS (e.g., NVDA, BTC, TSMC, ETH).\n"
                "2. A sector from BIT_CAPITAL_HOLDINGS (e.g., Semiconductors, Crypto Mining, AI Infrastructure).\n"
                "3. A macro theme from BIT_CAPITAL_HOLDINGS (e.g., Fed Rate Cuts, Semiconductor Export Controls).\n"
                "   NOTE: Macro theme matches must be EXACT. 'EU Tariffs on Chinese EVs' requires EU tariffs AND Chinese EVs specifically - US tariffs, general trade policy, or non-EV goods do NOT qualify.\n\n"
                "A market is NOT relevant if:\n"
                "- The connection requires more than one inferential leap.\n"
                "- It concerns general topics not directly linked to the holdings.\n"
                "- The information is vague or too broad.\n\n"
                "CRITICAL FORMATTING RULES:\n"
                "1. Your ENTIRE response MUST be a raw JSON array starting with '[' and ending with ']'.\n"
                "2. Do NOT include markdown code fences (no ```json or ```).\n"
                "3. Do NOT include any text or commentary outside the array.\n"
                "4. Do NOT call any tools - only return JSON.\n\n"
                f"BIT_CAPITAL_HOLDINGS:\n{self.llm.get_holdings_context()}\n\n"
                "Return a JSON array where each object contains:\n"
                "- 'market_id' (str)\n"
                "- 'is_relevant' (bool)\n"
                "- 'confidence' (float 0.0 - 1.0)\n"
                "- 'reason' (one sentence: why this market is relevant or not)\n"
            )

            prompt_parts = []
            for market in batch:
                prompt_parts.append(self._get_question_prompt(market))
                prompt_parts.append("-" * 30 + "\n") # Separator for readability in prompt
            prompt = "List of Markets to Filter:\n" + "".join(prompt_parts)

            try:
                response = await self.llm.get_json_completion(prompt, system=system_prompt)
                if isinstance(response, list):
                    return response
                else:
                    logger.error(f"Question batch expected list, got {type(response)}: {response}")
                    return []
            except Exception as e:
                market_ids = [m['market_id'] for m in batch]
                logger.error(f"Question batch failed for markets {market_ids}: {e}")
                return []

    async def _description_pass(self, market: dict) -> dict:
        async with self.semaphore2:
            # TODO: system prompt focused on deep analysis
            # prompt contains question + description + liquidity + probability + price_change
            # no batching - one market at a time
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
        # Pass 1 - minimal, question + tags only
        return (
            f"MARKET_ID: {market['market_id']}\n"
            f"Question: {market['question']}\n"
            f"Tags: {market.get('tags', [])}\n"
        )

    @staticmethod
    def _get_description_prompt(market: dict) -> str:
        # Pass 2 - focused on signal strength and analysis
        return (
            f"Question: {market['question']}\n"
            f"Description: {market.get('description', '')}\n"
            f"Probability: {market.get('probability')}\n"
            f"Liquidity: {market.get('liquidity', 0.0)}\n"
            f"Price change 24h: {market.get('price_change_day', 0.0)}\n"
            f"Price change week: {market.get('price_change_week', 0.0)}\n"
        )