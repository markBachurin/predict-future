import asyncio
import logging
from src.pss.storage.postgres.client import PostgresClient
from src.pss.llm.client import LLMClient
from src.pss.llm.classifier import MarketClassifier

logger = logging.getLogger(__name__)

def run_market_classification():
    """
    Airflow task callable to run the LLM classification pipeline.
    """
    pg_client = PostgresClient()
    llm_client = LLMClient()
    classifier = MarketClassifier(llm_client, pg_client)

    logger.info("Starting LLM classification task...")
    
    try:
        asyncio.run(classifier.classify_all())
        logger.info("LLM classification task completed successfully.")
    except Exception as e:
        logger.error(f"LLM classification task failed: {e}")
        raise
