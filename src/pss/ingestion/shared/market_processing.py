from src.pss.datatypes.raw_market import RawMarket
from src.pss.ingestion.shared.filter_markets import _process_market_data, _select_primary_market, _should_skip_event \
    , _extract_category, _build_raw_market_object, _keep_by_tags


def _parse_event(event: dict, logger) -> list[RawMarket]:
    results = []
    category = _extract_category(event)
    tags = [t.get("label", "") for t in event.get("tags", [])]
    title = event.get("title", "").lower()

    if _should_skip_event(category, tags, title, logger):
        return []

    if not _keep_by_tags(tags):
        return []

    parsed_markets_data = _process_market_data(event)

    best_market_data = _select_primary_market(parsed_markets_data)

    if not best_market_data:
        return []

    raw_market = _build_raw_market_object(
        best_market_data=best_market_data,
        event_data=event,
        category=category,
        tags=tags,
        logger=logger
    )
    results.append(raw_market)

    return results

