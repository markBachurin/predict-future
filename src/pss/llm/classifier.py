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

        # Pass 1, Gatekeeping
        market_batches = list(self._chunk_markets(markets, self.batch_size))
        gatekeeper_batch_tasks = [self._gatekeep_batch(batch) for batch in market_batches]
        gatekeeper_raw_results = await asyncio.gather(*gatekeeper_batch_tasks)

        gatekeeper_results_map = {}
        for batch_results in gatekeeper_raw_results:
            if isinstance(batch_results, list):
                for res in batch_results:
                    if "market_id" in res:
                        gatekeeper_results_map[res["market_id"]] = res
                    else:
                        logger.warning(f"Gatekeeper batch result missing 'market_id': {res}")
            else:
                logger.error(f"Unexpected gatekeeper batch result format: {batch_results}")

        relevant_markets = []
        for market in markets:
            market_id = market["market_id"]
            result = gatekeeper_results_map.get(market_id, {'is_relevant': False, "confidence": 0.0}) # Fallback for missing ID
            if result.get("is_relevant") and result.get("confidence", 0) > 0.7:
                market["gatekeeper_confidence"] = result.get("confidence")
                relevant_markets.append(market)

        logger.info(f"Gatekeeper filtered {len(markets)} -> {len(relevant_markets)} relevant markets.")

        # Pass 2, Reasoning
        if not relevant_markets:
            logger.info("No relevant markets for reasoning pass.")
            raw_ids = [m["raw_market_id"] for m in markets]
            self.pg.mark_processed(raw_ids)
            return

        relevant_market_batches = list(self._chunk_markets(relevant_markets, self.batch_size))
        reasoner_batch_tasks = [self._reason_batch(batch) for batch in relevant_market_batches]
        reasoner_raw_results = await asyncio.gather(*reasoner_batch_tasks)

        reasoner_results_map = {}
        for batch_results in reasoner_raw_results:
            if isinstance(batch_results, list):
                for res in batch_results:
                    if "market_id" in res:
                        reasoner_results_map[res["market_id"]] = res
                    else:
                        logger.warning(f"Reasoner batch result missing 'market_id': {res}")
            else:
                logger.error(f"Unexpected reasoner batch result format: {batch_results}")

        classifications = []
        for market in relevant_markets:
            market_id = market["market_id"]
            # Fallback uses existing error dict structure
            analysis = reasoner_results_map.get(market_id, {
                "tickers": [], "sectors": [], "direction": "neutral",
                "llm_confidence": 0.0, "foundational_details": "Analysis failed (missing from batch result).",
                "circumstances": "", "reasoning": "Market ID not found in LLM response batch."
            })

            score = self._calculate_weighted_score(market, analysis)

            classification = {
                "market_id": market["market_id"],
                "is_relevant": True,
                "tickers": analysis.get("tickers", []),
                "sectors": analysis.get("sectors", []),
                "direction": analysis.get("direction", "neutral"),
                "llm_confidence": analysis.get("llm_confidence", 0.0),
                "foundational_details": analysis.get("foundational_details", ""),
                "circumstances": analysis.get("circumstances", ""),
                "reasoning": analysis.get("reasoning", ""),
                "weighted_score": score
            }
            classifications.append(classification)

        if classifications:
            self.pg.insert_classifications(classifications)
            logger.info(f"Inserted {len(classifications)} classifications into database.")

        raw_ids = [m["raw_market_id"] for m in markets]
        self.pg.mark_processed(raw_ids)


    # private batching methods

    async def _gatekeep_batch(self, batch: list[dict]) -> list[dict]:
        async with self.semaphore1:
            system = (
                "You are a gatekeeper for BIT Capital, a tech-focused investment fund. "
                "Your job is to aggressively filter prediction markets. Be strict.\n\n"
                "A market is RELEVANT only if it meets ONE of these criteria:\n"
                "1. It directly names a ticker from BIT_CAPITAL_HOLDINGS (e.g. NVDA, BTC, TSMC).\n"
                "2. It directly names a sector from BIT_CAPITAL_HOLDINGS (e.g. Semiconductors, Crypto Mining).\n"
                "3. It directly names a macro theme from BIT_CAPITAL_HOLDINGS (e.g. Fed Rate Cuts, Semiconductor Export Controls).\n\n"
                "A market is NOT relevant if:\n"
                "- It concerns a company not in our tickers, even if that company operates in a related sector.\n"
                "- It concerns general US/global macroeconomics with no direct link to our holdings.\n"
                "- It concerns politics, sports, entertainment, or other non-portfolio topics.\n"
                "- The connection requires more than one inferential leap to reach a holding.\n\n"
                "Examples of NOT relevant:\n"
                "- 'Will xAI release Grok 5?' - xAI is not in our tickers.\n"
                "- 'Will the Fed cut rates?' - too broad, no direct holding link.\n"
                "- 'Will Tesla hit $500?' - Tesla is not in our tickers.\n\n"
                "Examples of RELEVANT:\n"
                "- 'Will NVDA beat earnings?' - directly names NVDA.\n"
                "- 'Will US impose new semiconductor export controls on China?' - directly names a macro theme.\n"
                "- 'Will Bitcoin hit $100k?' - directly names BTC.\n\n"
                "When in doubt, mark as NOT relevant. False negatives are acceptable. False positives waste resources.\n\n"
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
                "- 'reason' (one sentence: which specific ticker, sector, or macro theme makes this relevant or not)\n"
            )

            prompt_parts = []
            for market in batch:
                prompt_parts.append(f"MARKET_ID: {market['market_id']}\n")
                prompt_parts.append(self._get_prompt(market))
                prompt_parts.append("-" * 30 + "\n") # Separator for readability in prompt
            prompt = "List of Markets to Gatekeep:\n" + "".join(prompt_parts)

            try:
                response = await self.llm.get_json_completion(prompt, system=system)
                if isinstance(response, list):
                    return response
                else:
                    logger.error(f"Gatekeeper batch expected list, got {type(response)}: {response}")
                    return []
            except Exception as e:
                market_ids = [m['market_id'] for m in batch]
                logger.error(f"Gatekeeper batch failed for markets {market_ids}: {e}")
                return [] # Return empty list, results will be treated as not relevant


    async def _reason_batch(self, batch: list[dict]) -> list[dict]:
        async with self.semaphore2:
            system = (
                "You are a Senior Investment Analyst at BIT Capital. "
                "Every market you receive has already passed a strict relevance filter - it is guaranteed to connect to our portfolio. "
                "Your job is to determine HOW it connects and what the directional impact is.\n\n"
                "TICKER MAPPING RULES:\n"
                "- Start from the market event and trace the causal chain to our holdings.\n"
                "- If the market concerns a company NOT in our tickers but operating in our sectors, "
                "map it to the relevant tickers via sector exposure. "
                "For example: a market about xAI compute demand → AI Infrastructure sector → NVDA, AMD, TSMC.\n"
                "- You MUST always return at least one ticker and one sector. If you cannot, something is wrong with your reasoning.\n\n"
                "DIRECTION RULES:\n"
                "- Assign 'bullish' if the event, if it occurs, would likely increase demand, revenue, or sentiment for the ticker.\n"
                "- Assign 'bearish' if the event would likely decrease demand, revenue, or sentiment for the ticker.\n"
                "- Only use 'neutral' if the event genuinely has mixed or cancelling effects on the ticker.\n"
                "- Do NOT use 'neutral' as a default. Commit to a direction and explain it.\n\n"
                "CONFIDENCE RULES:\n"
                "- High confidence (0.8+): direct, clear causal chain with strong evidence.\n"
                "- Medium confidence (0.5-0.8): indirect but traceable causal chain.\n"
                "- Low confidence (below 0.5): speculative, multiple assumptions required.\n\n"
                "CRITICAL FORMATTING RULES:\n"
                "1. Your ENTIRE response MUST be a raw JSON array starting with '[' and ending with ']'.\n"
                "2. Do NOT include markdown code fences (no ```json or ```).\n"
                "3. Do NOT include any text or commentary outside the array.\n"
                "4. Do NOT call any tools - only return JSON.\n\n"
                f"BIT_CAPITAL_HOLDINGS:\n{self.llm.get_holdings_context()}\n\n"
                "Return a JSON array where each object contains:\n"
                "- 'market_id' (str)\n"
                "- 'tickers' (list: tickers from BIT_CAPITAL_HOLDINGS affected, at least one)\n"
                "- 'sectors' (list: sectors from BIT_CAPITAL_HOLDINGS affected, at least one)\n"
                "- 'direction' (bullish/bearish/neutral)\n"
                "- 'llm_confidence' (float 0-1)\n"
                "- 'confidence_reason' (one sentence: why this confidence level)\n"
                "- 'foundational_details' (one sentence: core facts of the market)\n"
                "- 'circumstances' (one sentence: macro/political triggers at play)\n"
                "- 'reasoning' (2-3 sentences: causal chain from market event to specific tickers)\n"
            )

            prompt_parts = []
            for market in batch:
                prompt_parts.append(f"MARKET_ID: {market['market_id']}\n")
                prompt_parts.append(self._get_prompt(market))
                prompt_parts.append("-" * 30 + "\n") # Separator for readability in prompt
            prompt = "List of Markets to Reason On:\n" + "".join(prompt_parts)

            try:
                response = await self.llm.get_json_completion(prompt, system=system)
                if isinstance(response, list):
                    return response
                else:
                    logger.error(f"Reasoner batch expected list, got {type(response)}: {response}")
                    return []
            except Exception as e:
                market_ids = [m['market_id'] for m in batch]
                logger.error(f"Reasoner batch failed for markets {market_ids}: {e}")
                return [] # Return empty list, results will be treated as failed analysis

    @staticmethod
    def _chunk_markets(markets: list[dict], batch_size: int) -> list[list[dict]]:
        """Yield successive n-sized chunks from list."""
        for i in range(0, len(markets), batch_size):
            yield markets[i:i + batch_size]

    @staticmethod
    def _calculate_weighted_score(market: dict, analysis: dict) -> float:
        llm_conf = Decimal(str(analysis.get("llm_confidence", 0.0))) # Convert to string first to ensure precision
        vol = max(market.get("volume", 0.0), settings.polymarket_volume_min)

        log_vol = math.log(float(vol))
        normal_vol_float = (log_vol - 0.0) / (16.1 - 10.8)
        normal_vol = Decimal(str(max(0, min(1.0, normal_vol_float))))

        price_change = abs(Decimal(market.get("price_change_day") or 0.0))

        norm_price = min(price_change / Decimal('0.1'), Decimal('1.0'))

        score = (llm_conf * Decimal('0.4')) + (normal_vol * Decimal('0.4')) + (norm_price * Decimal('0.2'))
        return round(float(score), 4)

    @staticmethod
    def _get_prompt(market: dict) -> str:
        return (
            f"Market Question: {market['question']}\n"
            f"Description: {market.get('description', '')}\n"
            f"Tags: {market.get('tags', [])} \n"
            f"Category: {market.get('category', '')}\n"
            f"Probability: {market.get('probability')}\n"
            f"Volume 24 hours: {market.get('volume24hr')}\n"
            f"Price change 24 hours: {market.get('price_change_day', 0.0)}\n"
            f"Price change week: {market.get('price_change_week', 0.0)}\n"
            f"Liquidity: {market.get('liquidity', 0.0)}\n"
            f"Outcomes: {market.get('outcomes', [])}\n"
            f"Outcome Probabilities: {market.get('outcome_probabilities', [])}\n"
        )