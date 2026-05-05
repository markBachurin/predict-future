from pss_config.config import settings
from src.pss.datatypes.raw_market import RawMarket
import json
from datetime import datetime


EXCLUDED_CATEGORIES = {
    "sports", "esports", "soccer", "nhl", "weather", "culture", "movies",
    "eurovision", "tennis", "music", "madrid open", "awards", "formula 1",
    "fifa world cup", "champions league", "games", "nba", "cristiano ronaldo",
    "reality tv", "netflix", "anime", "malta", "football", "league of legends",
    "celebrities", "pop culture", "entertainment", "boxing", "ufc", "horse racing",
    "cricket", "golf", "baseball", "rugby", "hockey"
}

NOISE_KEYWORDS = {
    "youtube", "tiktok", "instagram", "follower", "subscriber", "dating",
    "marriage", "divorce", "oscar", "grammy", "emmy", "mrbeast", "streamer",
    "box office", "trailer", "rotten tomatoes", "metacritic", "album",
    "concert", "tour", "leak", "spoiler", "death", "hospitalized"
}



def _should_skip_event(category: str | None, tags: list[str], title: str, logger) -> bool:
    category_lower = category.lower() if category else ""
    tags_lower = [t.lower() for t in tags]

    if category_lower in EXCLUDED_CATEGORIES or any(t in EXCLUDED_CATEGORIES for t in tags_lower):
        logger.debug(f"Skipping irrelevant category/tag: {category} | {tags}")
        return True

    if any(kw in title for kw in NOISE_KEYWORDS):
        logger.debug(f"Skipping event due to noise keyword: {title}")
        return True

    return False




def _select_primary_market(processed_markets_data: list[dict]) -> dict | None:
    # selects the market with prob closest to 0.5, falling back to highest volume
    if not processed_markets_data:
        return None

    best_market_data = None
    min_distance = 1.1

    for m_data in processed_markets_data:
        p = m_data["prob"]

        if p is not None:
            distance = abs(p - 0.5)
            if best_market_data is None or best_market_data["prob"] is None or distance < min_distance:
                min_distance = distance
                best_market_data = m_data
        else:
            # no prob: pick first seen, or highest volume if both lack prob
            if best_market_data is None or (best_market_data["prob"] is None and m_data["volume"] > best_market_data["volume"]):
                best_market_data = m_data

    return best_market_data





def _process_market_data(event: dict) -> list[dict]:
    processed_markets_data = []
    for market in event.get("markets", []):
        volume = float(event.get("volume") or market.get("volumeNum") or 0)
        if volume < settings.polymarket_volume_min:
            continue

        liquidity = float(market.get("liquidity") or 0)
        if liquidity < settings.polymarket_liquidity_min:
            continue

        market_type = market.get("market_type") or market.get("marketType")
        outcomes_raw = market.get("outcomes", [])
        outcomes_list = json.loads(outcomes_raw) if isinstance(outcomes_raw, str) else (outcomes_raw or [])

        if not market_type:
            market_type = "Binary" if len(outcomes_list) == 2 else "Multi-Outcome"

        probs_raw = market.get("outcomePrices") or market.get("probabilities") or []
        probs_list = json.loads(probs_raw) if isinstance(probs_raw, str) else (probs_raw or [])
        outcome_probabilities_list = []
        for p in probs_list:
            try:
                if p is not None:
                    outcome_probabilities_list.append(float(p))
            except (ValueError, TypeError):
                continue

        # extract single probability for Binary markets
        prob = None
        if market_type == "Binary" and len(outcomes_list) == 2 and len(outcome_probabilities_list) == 2:
            yes_index = outcomes_list.index("Yes") if "Yes" in outcomes_list else 0
            prob = outcome_probabilities_list[yes_index]

        if prob is not None and not (0.0 <= prob <= 1.0):
            prob = None

        processed_markets_data.append({
            "market": market,
            "market_type": market_type,
            "outcomes": outcomes_list,
            "probs": outcome_probabilities_list,
            "prob": prob,
            "volume": volume,
            "liquidity": liquidity
        })
    return processed_markets_data



def _parse_expiry(end_date_str: str | None, logger) -> datetime | None:
    if not end_date_str:
        return None
    try:
        return datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
    except ValueError as e:
        logger.error(f"_parse_expiry error : {e}")
        return None




def _build_raw_market_object(best_market_data: dict, event_data: dict, category: str | None, tags: list[str], logger) -> RawMarket:
    m = best_market_data["market"]
    return RawMarket(
        source="polymarket",
        external_id=f"polymarket:{m.get('conditionId', m.get('id'))}",
        question=m.get("question", event_data.get("title", "")),
        description=m.get("description") or event_data.get("description"),
        probability=best_market_data["prob"],
        volume=best_market_data["volume"],
        category=category,
        expiry=_parse_expiry(event_data.get("endDate"), logger),
        volume24hr=float(m.get("volume24hr") or 0),
        price_change_day=float(m.get("oneDayPriceChange")) if m.get("oneDayPriceChange") is not None else None,
        price_change_week=float(m.get("oneWeekPriceChange")) if m.get("oneWeekPriceChange") is not None else None,
        liquidity=best_market_data["liquidity"],
        tags=tags,
        market_type=best_market_data["market_type"],
        outcomes=best_market_data["outcomes"],
        outcome_probabilities=best_market_data["probs"],
        resolution_source=m.get("resolution_source") or m.get("resolutionSource"),
        ticker=event_data.get("ticker"),
        restricted=m.get("restricted", False),
    )




def _parse_event(event: dict, logger) -> list[RawMarket]:
    results = []
    category = _extract_category(event)
    tags = [t.get("label", "") for t in event.get("tags", [])]
    title = event.get("title", "").lower()

    if _should_skip_event(category, tags, title, logger):
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



def _extract_category(event: dict) -> str | None:
    tags = event.get("tags", [])
    return tags[0].get("label") if tags else None