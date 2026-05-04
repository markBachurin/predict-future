import logging
import asyncio
from src.pss.llm.client import LLMClient
from src.pss.storage.postgres.client import PostgresClient

logger = logging.getLogger(__name__)

class MarketClassiefier:
    def __init__(self, llm_client: LLMClient, pg_client: PostgresClient):
        self.llm = llm_client
        self.pg = pg_client
        self.semaphore = asyncio.Semaphore(50)

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

    # private

    # semaphore:
    async def _gatekeep_market(self, m: dict) -> dict:
        async with self.semaphore:
            return await self._unit_gatekeep_market(m)

    async def _unit_gatekeep_market(self, market: dict) -> dict:
        system = (
            "You are a gatekeeper for BIT Capital, a tech-focused investment fund."
            "Your job is to determine if a prediction market is RELEVANT to our portfolio holdings or focus sectors. \n\n"
            f"HOLDINGS & SECTORS: \n {self.llm.get_holdings_context()}\n\n"
            "Return a JSON object with 'is_relevant' (bool) and 'confidence' (float 0.0 - 1.0)."
        )

        prompt = (
            f"Market Question: {market['question']}\n"
            f"Description: {market.get('description', '')}\n"
            f"Tags: {market.get('tags'), []} \n"
            f"Category: {market.get('category', '')}\n"
        )

        try:
            return await self.llm.get_json_completion(prompt, system=system, model=self.llm.gatekeeper_model)
        except Exception as e:
            logger.error(f"Gatekeeper failed for market {market['market_id']} :{e}")
            return {'is_relevant': False, "confidence": 0.0}