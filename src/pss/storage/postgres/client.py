import psycopg2
from psycopg2.extras import execute_values, Json, RealDictCursor
from contextlib import contextmanager
from pss_config.config import settings
from src.pss.datatypes.raw_market import RawMarket
from src.pss.datatypes.validated_market import ValidatedMarket
from src.pss.storage.shared.client import Client
from typing import Union

Market = Union[RawMarket, ValidatedMarket]

class PostgresClient(Client):
    def upload_markets(self, markets: list[Market]) -> list[str]:
        if not markets:
            return []

        with self._get_conn() as conn:
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

    def upsert_markets(self, raw_ids: list[str], markets: list[ValidatedMarket], is_valid: bool) -> list[str]:
        if not markets:
            return []

        with self._get_conn() as conn:
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
                            probability = EXCLUDED.probability,
                            volume = EXCLUDED.volume,
                            volume24hr = EXCLUDED.volume24hr,
                            price_change_day = EXCLUDED.price_change_day,
                            price_change_week = EXCLUDED.price_change_week,
                            liquidity = EXCLUDED.liquidity,
                            tags = EXCLUDED.tags,
                            description = EXCLUDED.description,
                            category = EXCLUDED.category,
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

    def insert_snapshots(self, market_ids: list[str], markets: list[ValidatedMarket]) -> None:
        if not markets:
            return

        with self._get_conn() as conn:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                        INSERT INTO market_snapshots (market_id, probability, outcome_probabilities, volume, volume24hr, price_change_day, price_change_week)
                        VALUES %s
                    """,
                    [
                        (market_ids[i], m.probability, m.outcome_probabilities, m.volume, m.volume24hr, m.price_change_day, m.price_change_week)
                        for i, m in enumerate(markets)
                    ]
                )

    def get_markets_for_classification(self) -> list[dict]:
        with self._get_conn() as conn:
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
                    
                    LIMIT 50
                """)
                return [dict(row) for row in cur.fetchall()]


    def mark_processed(self, raw_market_ids: list[str]) -> None:
        if not raw_market_ids:
            return

        with self._get_conn() as conn:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    "UPDATE raw_markets SET processed = true WHERE id = %s",
                    [(id,) for id in raw_market_ids]
                )

    def insert_classifications(self, results: list[dict]) -> None:
        if not results:
            return

        with self._get_conn() as conn:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                        INSERT INTO llm_classifications (
                            market_id, is_relevant, tickers, sectors, direction,
                            foundational_details, circumstances, reasoning, 
                            llm_confidence, weighted_score
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
                            weighted_score = EXCLUDED.weighted_score,
                            classified_at = now()
                    """,
                    [
                        (
                            r['market_id'], r['is_relevant'], r['tickers'], r['sectors'], r['direction'],
                            r.get('foundational_details'), r.get('circumstances'), r['reasoning'],
                            r['llm_confidence'], r['weighted_score']
                        )
                        for r in results
                    ]
                )

    # private methods:

    @staticmethod
    def _get_connection():
        return psycopg2.connect(
            host=settings.db_host,
            port=settings.db_port,
            dbname=settings.db,
            user=settings.db_user,
            password=settings.db_password,
            connect_timeout=10,
            sslmode="require",
        )

    @contextmanager
    def _get_conn(self):
        conn = self._get_connection()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
