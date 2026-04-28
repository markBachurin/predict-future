import psycopg2
from psycopg2.extras import execute_values, Json
from contextlib import contextmanager
from config.config import settings
from src.pss.ingestion.shared.base import RawMarket
from src.pss.storage.shared.client import Client
from typing import Any

class Postgres_Client(Client):
    def upload_markets(self, markets: list[RawMarket]) -> list[str]:
        if not markets:
            return []

        with self._get_conn() as conn:
            with conn.cursor() as cur:
                rows = execute_values(
                    cur,
                    """
                        INSERT INTO raw_markets (source, external_id, raw_payload)
                        VALUES %s
                        ON CONFLICT (source, external_id, ingested_at) DO NOTHING
                        RETURNING id
                    """,
                    [
                        (market.source, market.external_id, Json(market.raw_payload))
                        for market in markets
                    ],
                    fetch=True
                )
                return [str(row[0]) for row in rows]

    def upsert_market(self, raw_id: str | Any, market: RawMarket, is_valid: bool) -> str | None:
        if not market or not raw_id:
            return None

        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                                INSERT INTO markets(raw_market_id, source, external_id, question, category,
                                probability, volume, expiry, is_valid, normalized_at
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                                ON CONFLICT (source, external_id) DO UPDATE SET
                                    probability = EXCLUDED.probability,
                                    volume = EXCLUDED.volume,
                                    category = EXCLUDED.category, 
                                    updated_at = now()
                                RETURNING id
                            """, (
                    raw_id,
                    market.source,
                    market.external_id,
                    market.question,
                    market.category,
                    market.probability,
                    market.volume,
                    market.expiry,
                    is_valid,
                ))
                return  str(cur.fetchone()[0])

    def insert_snapshot(self, market_id: str | Any, market: RawMarket) -> str | None:
        if not market_id or not market:
            return None
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO market_snapshots (market_id, probability, volume)
                    VALUES (%s, %s, %s)
                    RETURNING id
                """, (market_id, market.probability, market.volume))
                return str(cur.fetchone()[0])


    # private methods:

    @staticmethod
    def _get_connection():
        return psycopg2.connect(settings.database_url)

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
