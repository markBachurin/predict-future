from psycopg2.extras import execute_values,  RealDictCursor
from src.pss.datatypes.raw_market import RawMarket
from src.pss.datatypes.validated_market import ValidatedMarket
from typing import Union
import logging

Market = Union[RawMarket, ValidatedMarket]
logger = logging.getLogger(__name__)

def _upload_markets(markets: list[Market], connection) -> list[str]:
    if not markets:
        return []

    with connection as conn:
        with conn.cursor() as cur:
            rows = execute_values(
                cur,
                """
                    INSERT INTO raw_markets (source, external_id)
                    VALUES %s
                    ON CONFLICT (source, external_id, ingested_at) DO UPDATE
                        SET source = EXCLUDED.source
                    RETURNING id
                """,
                [
                    (market.source, market.external_id)
                    for market in markets
                ],
                fetch=True
            )
            return [str(row[0]) for row in rows]


def _upsert_markets(raw_ids: list[str], markets: list[ValidatedMarket], is_valid: bool, connection) -> list[str]:
    if not markets:
        return []

    with connection as conn:
        with conn.cursor() as cur:
            rows = execute_values(
                cur,
                """
                    INSERT INTO markets(raw_market_id, source, external_id, question, description,
                    category, probability, volume, volume24hr, price_change_day, price_change_week,
                    liquidity, tags, expiry, is_valid, market_type, outcomes, outcome_probabilities, resolution_source, 
                    ticker, restricted)
                    VALUES %s
                    ON CONFLICT (source, external_id) DO UPDATE SET
                        raw_market_id = EXCLUDED.raw_market_id,
                        question = EXCLUDED.question,
                        probability = EXCLUDED.probability,
                        volume = EXCLUDED.volume,
                        volume24hr = EXCLUDED.volume24hr,
                        price_change_day = EXCLUDED.price_change_day,
                        price_change_week = EXCLUDED.price_change_week,
                        liquidity = EXCLUDED.liquidity,
                        tags = EXCLUDED.tags,
                        description = EXCLUDED.description,
                        category = EXCLUDED.category,
                        is_valid = EXCLUDED.is_valid,
                        market_type = EXCLUDED.market_type,
                        outcomes = EXCLUDED.outcomes, 
                        outcome_probabilities = EXCLUDED.outcome_probabilities, 
                        resolution_source = EXCLUDED.resolution_source, 
                        ticker = EXCLUDED.ticker, 
                        restricted = EXCLUDED.restricted, 
                        updated_at = now()
                    RETURNING id
                """,
                [
                    (
                        raw_ids[i], m.source, m.external_id, m.question, m.description,
                        m.category, m.probability, m.volume, m.volume24hr, m.price_change_day,
                        m.price_change_week, m.liquidity, m.tags, m.expiry, is_valid, m.market_type,
                        m.outcomes, m.outcome_probabilities, m.resolution_source, m.ticker, m.restricted,
                    )
                    for i, m in enumerate(markets)
                ],
                fetch=True
            )
            return [str(row[0]) for row in rows]


def _get_markets_for_classification(connection) -> list[dict]:
    with connection as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT 
                    m.id as market_id,
                    m.question,
                    m.description,
                    m.tags,
                    m.category,
                    m.probability,
                    m.volume,
                    m.volume24hr,
                    m.price_change_day,
                    m.price_change_week,
                    m.liquidity,
                    m.outcomes,
                    m.outcome_probabilities,
                    r.id as raw_market_id
                FROM markets m
                JOIN raw_markets r ON m.raw_market_id = r.id
                LEFT JOIN llm_classifications lc ON m.id = lc.market_id
                WHERE r.processed = false
                AND lc.id IS NULL
            """)
            return [dict(row) for row in cur.fetchall()]


def _mark_processed(raw_market_ids: list[str], connection) -> None:
    if not raw_market_ids:
        return

    with connection as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE raw_markets SET processed = true WHERE id IN %s",
                (tuple(raw_market_ids),)
            )


def _insert_classifications(results: list[dict], connection) -> None:
    if not results:
        return

    with connection as conn:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                    INSERT INTO llm_classifications (
                        market_id, is_relevant, tickers, sectors, direction,
                        foundational_details, circumstances, reasoning, 
                        llm_confidence, confidence_reason, question_filter_confidence, weighted_score
                    )
                    VALUES %s
                    ON CONFLICT (market_id) DO UPDATE SET
                        is_relevant = EXCLUDED.is_relevant,
                        tickers = EXCLUDED.tickers,
                        sectors = EXCLUDED.sectors,
                        direction = EXCLUDED.direction,
                        foundational_details = EXCLUDED.foundational_details,
                        circumstances = EXCLUDED.circumstances,
                        reasoning = EXCLUDED.reasoning,
                        llm_confidence = EXCLUDED.llm_confidence,
                        confidence_reason = EXCLUDED.confidence_reason,
                        question_filter_confidence = EXCLUDED.question_filter_confidence,
                        weighted_score = EXCLUDED.weighted_score,
                        classified_at = now()
                """,
                [
                    (
                        r['market_id'], r['is_relevant'], r['tickers'], r['sectors'], r['direction'],
                        r.get('foundational_details'), r.get('circumstances'), r['reasoning'],
                        r['llm_confidence'], r.get('confidence_reason'), r["question_filter_confidence"], r['weighted_score']
                    )
                    for r in results
                ]
            )

def _insert_pass_results(results_map: dict, pass_number: int, connection) -> None:
    if not results_map:
        return None

    rows = []
    for market_id, result in results_map.items():
        if pass_number == 1:
            rows.append(
                (
                    market_id,
                    pass_number,
                    result.get("is_relevant"),
                    result.get("confidence"),
                    result.get("confidence_reason"),
                    result.get("reason"),
                )
            )
        elif pass_number == 2:
            rows.append(
                (
                    market_id,
                    pass_number,
                    True,
                    result.get("llm_confidence"),
                    result.get("confidence_reason"),
                    result.get("reasoning"),
                )
            )
        else:
            logger.error(f"Unknown pass_number: {pass_number}")

    with connection as conn:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                    INSERT INTO llm_pass_results 
                        (market_id, pass_number, is_relevant, confidence, confidence_reason, reason)
                    VALUES %s
                """,
                rows
            )

def _drop_db(connection) -> None:
    with connection as conn:
        with conn.cursor() as cur:
            cur.execute("""
                DROP TABLE IF EXISTS llm_classifications;
                DROP TABLE IF EXISTS llm_pass_results;
                DROP TABLE IF EXISTS markets;
                DROP TABLE IF EXISTS raw_markets;
            """)



