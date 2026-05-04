import logging
import asyncio
from src.pss.llm.client import LLMClient
from src.pss.storage.postgres.client import PostgresClient

logger = logging.getLogger(__name__)

class MarketClassiefier:
    def __init__(self, llm_client: LLMClient, pg_client: PostgresClient):
        self.llm = llm_client,
        self.pg = pg_client

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
    def _gatekeep_market(self, market: dict) -> dict:
        ...