import logging
from src.pss.datatypes.raw_market import RawMarket
from src.pss.datatypes.validated_market import ValidatedMarket

logger = logging.getLogger(__name__)

def validate_market(market: RawMarket) -> ValidatedMarket | None:
    try:
        return ValidatedMarket(
            source = market.source,
            external_id= market.external_id,
            question= market.question,
            probability= market.probability,
            volume= market.volume,
            category= market.category,
            expiry= market.expiry,
            raw_payload= market.raw_payload,

        )
    except Exception as e:
        logger.warning(f"Dropping invalid market  [{market.source}:{market.external_id}] - {e}")
        return None


def validate_markets(markets: list[RawMarket]) -> list[ValidatedMarket]:
    results = []
    dropped = 0

    for market in markets:
        validated = validate_market(market)
        if validated:
            results.append(validated)
        else:
            dropped += 1

    logger.info(
        f"Validation complete - {len(results)} valid, {dropped} dropped out of {len(markets)} total"
    )
    return results