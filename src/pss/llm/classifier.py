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

        # Insert results of the first pass into the database
        self.pg.insert_pass_results(question_filter_results_map, pass_number=1)

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


        # Pass 2 - Description reasoning
        description_pass_tasks = []
        for market in relevant_markets:
            description_pass_tasks.append(self._description_pass(market))
        
        description_pass_raw_results = await asyncio.gather(*description_pass_tasks)

        description_results_map = {}
        successful_analyses_count = 0
        for i, res in enumerate(description_pass_raw_results):
            market_id = relevant_markets[i]["market_id"]
            if isinstance(res, dict) and "error" not in res:
                description_results_map[market_id] = res
                successful_analyses_count += 1
            else:
                logger.warning(f"Description pass failed for market {market_id}: {res}")

        logger.info(f"Description pass: {len(relevant_markets)} relevant markets -> {successful_analyses_count} successful analyses.")

        # Insert results of the second pass into the database
        if description_results_map:
            self.pg.insert_pass_results(description_results_map, pass_number=2)
            logger.info(f"Inserted {len(description_results_map)} Pass 2 results.")

        classifications = []
        for market in relevant_markets:
            market_id = market["market_id"]
            analysis_result = description_results_map.get(market_id)

            if analysis_result:
                try:
                    classification_dict = self._get_classifications_dict(market_id, analysis_result, market)
                    classification_dict["weighted_score"] = self._calculate_weighted_score(market, classification_dict)
                    classifications.append(classification_dict)
                except Exception as e:
                    logger.error(f"Error creating classification for market {market_id} from analysis result: {e}. Result: {analysis_result}")
            else:
                logger.warning(f"No successful analysis result found for market {market_id} in Pass 2. Skipping classification.")

        if classifications:
            self.pg.insert_classifications(classifications)
            logger.info(f"Inserted {len(classifications)} final classifications.")
        else:
            logger.info("No classifications to insert after Pass 2.")

        # Mark all initial markets as processed
        self.pg.mark_processed(all_raw_market_ids)
        logger.info(f"Marked {len(all_raw_market_ids)} raw markets as processed.")

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
                "- 'confidence_reason' (one sentence: why this confidence score was chosen)\n"
            )

            prompt_parts = []
            for market in batch:
                prompt_parts.append(self._get_question_prompt(market))
                prompt_parts.append("-" * 30 + "\n") # Separator for readability in prompt
            prompt = "List of Markets to Filter:\n" + "".join(prompt_parts)

            try:
                response = await self.llm.get_json_completion(prompt, system=system_prompt)
                if isinstance(response, list):
                    # Ensure confidence_reason is captured if present
                    for res in response:
                        if 'confidence_reason' not in res:
                            res['confidence_reason'] = None
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
            system_prompt = (
                "You are an expert financial analyst for BIT Capital, specializing in prediction market analysis. "
                "The market provided has already been filtered as relevant to BIT Capital's interests. "
                "Your primary task is to deeply analyze this single market (question, description, probability, liquidity, price changes) "
                "and determine its connection to BIT Capital's holdings, assess its signal strength and direction, and evaluate its urgency. "
                "You must map the market event to specific tickers and sectors from the provided BIT_CAPITAL_HOLDINGS. "
                "Commit to a clear direction (bullish/bearish/neutral) and do NOT default to neutral if a direction can be inferred. "
                "Assess urgency: high probability (>0.7) combined with rising price indicates a strong signal; "
                "low probability (<0.3) with falling price suggests a weak or contrarian signal. "
                "Price changes (24hr and weekly) are crucial indicators of market momentum and urgency.\n\n"
                "CRITICAL FORMATTING RULES:\n"
                "1. Your ENTIRE response MUST be a raw JSON object starting with '{' and ending with '}'.\n"
                "2. Do NOT include markdown code fences (no ```json or ```).\n"
                "3. Do NOT include any text or commentary outside the object.\n"
                "4. Do NOT call any tools - only return JSON.\n\n"
                f"BIT_CAPITAL_HOLDINGS:\n{self.llm.get_holdings_context()}\n\n"
                "Return a JSON object containing these fields:\n"
                "- 'market_id' (str): The unique identifier of the market. \n"
                "- 'tickers' (list[str]): At least one ticker from BIT_CAPITAL_HOLDINGS that is directly impacted. \n"
                "- 'sectors' (list[str]): At least one sector from BIT_CAPITAL_HOLDINGS that is directly impacted. \n"
                "- 'direction' (str): 'bullish', 'bearish', or 'neutral' (commit to one). \n"
                "- 'llm_confidence' (float): Your confidence in the analysis, from 0.0 to 1.0. \n"
                "- 'confidence_reason' (str): A single sentence explaining the chosen confidence level. \n"
                "- 'foundational_details' (str): A single sentence summarizing the core facts of the market. \n"
                "- 'circumstances' (str): A single sentence describing the macro or political triggers at play. \n"
                "- 'reasoning' (str): 2-3 sentences detailing the causal chain from the market event to the identified tickers/sectors and their expected impact. \n"
            )

            prompt = self._get_description_prompt(market)

            try:
                response = await self.llm.get_json_completion(prompt, system=system_prompt)
                if isinstance(response, dict):
                    if "market_id" not in response:
                        response["market_id"] = market["market_id"]
                    return response
                else:
                    logger.error(f"Description pass for market {market.get('market_id')} expected dict, got {type(response)}: {response}")
                    return {"market_id": market["market_id"], "error": "Unexpected LLM response format"}
            except Exception as e:
                logger.error(f"Description pass failed for market {market.get('market_id')}: {e}")
                return {"market_id": market["market_id"], "error": str(e)}

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

    @staticmethod
    def _get_classifications_dict(market_id: str, analysis_result: dict | None, market: dict) -> dict:
        return {
            "market_id": market_id,
            "is_relevant": True,
            "tickers": analysis_result.get("tickers"),
            "sectors": analysis_result.get("sectors"),
            "direction": analysis_result.get("direction"),
            "llm_confidence": analysis_result.get("llm_confidence"),
            "confidence_reason": analysis_result.get("confidence_reason"),
            "foundational_details": analysis_result.get("foundational_details"),
            "circumstances": analysis_result.get("circumstances"),
            "reasoning": analysis_result.get("reasoning"),
            "question_filter_confidence": market.get("question_filter_confidence")
        }