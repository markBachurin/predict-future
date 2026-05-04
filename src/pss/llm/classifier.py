import logging
import asyncio
from src.pss.llm.client import LLMClient
from src.pss.storage.postgres.client import PostgresClient
from pss_config.config import settings

logger = logging.getLogger(__name__)

class MarketClassifier:
    def __init__(self, llm_client: LLMClient, pg_client: PostgresClient):
        self.llm = llm_client
        self.pg = pg_client
        self.semaphore1 = asyncio.Semaphore(settings.gatekeep_thread_limit)
        self.semaphore2 = asyncio.Semaphore(settings.reason_thread_limit)

    async def classify_all(self):
        # main entry point for classification pipeline
        markets = self.pg.get_markets_for_classification()
        if not markets:
            logger.info("No new markets to classify.")
            return

        logger.info(f"Starting to classify {len(markets)} markets")

        # pass 1, gatekeeping
        gatekeeper_tasks = [self._gatekeep_market(m) for m in markets]
        gatekeeper_results = await asyncio.gather(*gatekeeper_tasks)

        relevant_markets = []
        for market, result in zip(markets, gatekeeper_results):
            if result.get("is_relevant") and result.get("confidence", 0) > 0.7:
                market["gatekeeper_confidence"] = result.get("confidence")
                relevant_markets.append(market)

        logger.info(f"Gatekeeper filtered {len(markets)} -> {len(relevant_markets)} relevant markets.")

        if not relevant_markets:
            # mark everything as processed even if not relevant
            raw_ids = [m["raw_market_id"] for m in markets]
            self.pg.mark_processed(raw_ids)

        # pass 2, reasoning
        reasoner_tasks = [self._reason_market(m) for m in relevant_markets]
        reasoner_results = await asyncio.gather(*reasoner_tasks)

        classifications = []
        for market, analysis in zip(relevant_markets, reasoner_results):
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


    # private

    # semaphore:
    async def _gatekeep_market(self, m: dict) -> dict:
        async with self.semaphore1:
            return await self._unit_gatekeep_market(m)

    async def _unit_gatekeep_market(self, market: dict) -> dict:
        system = (
            "You are a gatekeeper for BIT Capital, a tech-focused investment fund."
            "Your job is to determine if a prediction market is RELEVANT to our portfolio holdings or focus sectors. \n\n"
            f"HOLDINGS & SECTORS: \n {self.llm.get_holdings_context()}\n\n"
            "Return a JSON object with 'is_relevant' (bool) and 'confidence' (float 0.0 - 1.0)."
        )

        prompt = self._get_prompt(market)

        try:
            return await self.llm.get_json_completion(prompt, system=system, model=self.llm.gatekeeper_model)
        except Exception as e:
            logger.error(f"Gatekeeper failed for market {market['market_id']} :{e}")
            return {'is_relevant': False, "confidence": 0.0}


    async def _reason_market(self, market: dict) -> dict:
        async with self.semaphore2:
            return await self._unit_reason_market(market)

    async def _unit_reason_market(self, market: dict) -> dict:
        system = (
            "You are a Senior Investment Analyst at BIT Capital. "
            "Analyze the following prediction market and its potential impact on our portfolio. \n\n"
            F"HOLDINGS & SECTORS: \n{self.llm.get_holdings_context()}\n\n"
            "Analyze the sentiment (bullish/bearish) for the specific tickers involved. "
            "If the market probability is high (>0.7) for an event that helps a ticker, it's bullish."
            "If it's low (<0.3) for a helpful event, it's bearish.\n"
            "Return a JSON obect with:\n"
            "- 'tickers' (list of identified tickers from BIT Capital holdings)\n"
            "- 'sectors' (list of relevant sectors)\n"
            "- 'direction' (bullish/bearish/neutral)\n"
            "- 'llm_confidence' (float 0-1, how certain you are of this impact)\n"
            "- 'foundational_details' (brief string: the core facts of the market)\n"
            "- 'circumstances' (brief string: what specific macro/political triggers are at play)\n"
            "- 'reasoning' (detailed string: why this matters for the tickers involved)"
        )

        prompt = self._get_prompt(market)

        try:
            return await self.llm.get_json_completion(prompt, system=system, model=self.llm.reasoner_model)
        except Exception as e:
            logger.error(f"Reasoner failed for market {market['market_id']}: {e}")
            return {
                "tickers": [],
                "sectors": [],
                "direction": "neutral",
                "llm_confidence": 0.0,
                "foundational_details": "Analysis failed.",
                "circumstances": "",
                "reasoning": str(e)
            }

    @staticmethod
    def _calculate_weighted_score(market: dict, analysis: dict) -> float:
        import math

        llm_conf = analysis.get("llm_confidence", 0.0)
        vol = max(market.get("volume", 0.0), settings.polymarket_volume_min)

        log_vol = math.log(vol)
        normal_vol = (log_vol - 0.0) / (16.1 - 10.8)
        normal_vol = max(0, min(1.0, normal_vol))

        price_change = abs(market.get("price_change_day", 0.0))

        norm_price = min(price_change / 0.1, 1.0)

        score = (llm_conf * 0.4) + (normal_vol * 0.4) + (norm_price * 0.2)
        return round(score, 4)

    @staticmethod
    def _get_prompt(market: dict) -> str:
        return (
            f"Market Question: {market['question']}\n"
            f"Description: {market.get('description', '')}\n"
            f"Tags: {market.get('tags'), []} \n"
            f"Category: {market.get('category', '')}\n"
            f"Probability: {market.get('probability')}\n"
            f"Volume 24 hours: {market.get('volume24hr')}\n"
            f"Price change 24 hours: {market.get('price_change_day', 0.0)}\n"
            f"Price change week: {market.get('price_change_week', 0.0)}\n"
            f"Liquidity: {market.get('liquidity', 0.0)}\n"
            f"Outcomes: {market.get('outcomes', [])}\n"
            f"Outcome Probabilities: {market.get('outcome_probabilities', [])}\n"
        )