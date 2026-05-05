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
                "You are a gatekeeper for BIT Capital, a tech-focused investment fund."
                "Your primary role is to determine if each prediction market in the provided list is RELEVANT to our specific investment portfolio and focus areas.\n\n"
                "A market is ONLY considered RELEVANT if it has a DIRECT and SPECIFIC connection to one or more of the exact tickers, sectors, or macro themes listed in BIT_CAPITAL_HOLDINGS. Loose macroeconomic associations or general economic trends do NOT qualify as relevant unless they directly impact a specific holding.\n\n"
                "A market about a company not listed in our tickers is NOT relevant, even if that company operates in a related sector.\n"
                "For example:\n"
                "- A market predicting a US bank failure is NOT relevant because BIT Capital holds no banking stocks.\n"
                "- A market predicting a general US court tariff ruling is NOT relevant to AUTO1, because AUTO1's exposure is specifically EU tariffs on Chinese EVs, not broad US import tariff policy.\n\n"
                "If a market is relevant, you MUST identify the specific ticker(s), sector(s), or macro_theme(s) that make it relevant in the 'reason' field.\n"
                "If NO specific ticker, sector, or macro_theme is directly affected, 'is_relevant' MUST be false.\n\n"
                "\nCRITICAL FORMATTING RULES:\n"
                "1. Your ENTIRE response MUST be a raw JSON array starting with '[' and ending with ']'.\n"
                "2. Do NOT include markdown code fences (no ```json or ```).\n"
                "3. Do NOT include any preamble, text, commentary, or agent commands before or after the array.\n"
                "4. Do NOT call any functions or tools — only return JSON.\n\n"
                f"HOLDINGS & SECTORS: \n {self.llm.get_holdings_context()}\n\n"
                "Return a JSON array of objects. Each object must contain 'market_id' (str), 'is_relevant' (bool), 'confidence' (float 0.0 - 1.0), and 'reason' (brief string explaining why this market is or isn't relevant to our holdings, referencing specific holdings where applicable)."
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
                "Your task is to analyze each prediction market and determine its precise impact on our portfolio holdings.\n\n"
                "You MUST ONLY identify tickers from the exact list provided in BIT_CAPITAL_HOLDINGS.\n"
                "For each relevant market, you MUST explicitly state in the 'reasoning' field:\n"
                "1. Which specific ticker(s) from BIT_CAPITAL_HOLDINGS are affected.\n"
                "2. Which specific sector(s) are affected.\n"
                "3. Which specific macro_theme(s) are at play.\n"
                "4. WHY these are affected by the market event.\n\n"
                "Be precise about the sentiment direction:\n"
                "- 'bullish' or 'bearish' is ONLY appropriate if there is a clear, direct causal mechanism linking the market event to the specified holding's performance.\n"
                "- If the connection is indirect, speculative, or uncertain, use 'neutral'.\n"
                "- Vague macroeconomic connections or general market sentiment are NOT sufficient grounds for assigning 'bullish' or 'bearish'. The reasoning MUST trace a clear causal chain from the market event to a specific holding's likely performance.\n\n"
                "Your ENTIRE response MUST be a single valid JSON array. Do not include any explanatory text, markdown, agent commands, or any content outside of this JSON array.\n\n"
                "CRITICAL FORMATTING RULES:\n"
                "1. Your ENTIRE response MUST be a raw JSON array starting with '[' and ending with ']'.\n"
                "2. Do NOT include markdown code fences (no ```json or ```).\n"
                "3. Do NOT include any text, commentary, or agent commands before or after the array.\n"
                "4. Do NOT call any functions or tools — only return JSON.\n\n"
                "Return a JSON array of objects. Each object must contain:\n"
                "- 'market_id' (str)\n"
                "- 'tickers' (list of identified tickers from BIT Capital holdings)\n"
                "- 'sectors' (list of relevant sectors)\n"
                "- 'direction' (bullish/bearish/neutral)\n"
                "- 'llm_confidence' (float 0-1, how certain you are of this impact)\n"
                "- 'confidence_reason' (brief string: why you assigned this confidence level)\n"
                "- 'foundational_details' (brief string: the core facts of the market)\n"
                "- 'circumstances' (brief string: what specific macro/political triggers are at play)\n"
                "- 'reasoning' (detailed string: why this matters for the tickers involved, tracing the causal chain)\n\n"
                f"HOLDINGS & SECTORS:\n{self.llm.get_holdings_context()}\n\n"
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