import psycopg2
from psycopg2.extras import execute_values, Json
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
                        INSERT INTO markets(raw_market_id, source, external_id, question, category,
                        probability, volume, expiry, is_valid)
                        VALUES %s
                        ON CONFLICT (source, external_id) DO UPDATE SET
                            probability = EXCLUDED.probability,
                            volume = EXCLUDED.volume,
                            category = EXCLUDED.category,
                            updated_at = now()
                        RETURNING id
                    """,
                    [
                        (raw_ids[i], m.source, m.external_id, m.question, m.category, m.probability, m.volume, m.expiry, is_valid)
                        for i, m in enumerate(markets)
                    ],
                    fetch=True
                )
                return [str(row[0]) for row in rows]

    def insert_snapshots(self, market_ids: list[str], markets: list[ValidatedMarket]) -> list[str]:
        if not markets:
            return []

        with self._get_conn() as conn:
            with conn.cursor() as cur:
                rows = execute_values(
                    cur,
                    """
                        INSERT INTO market_snapshots (market_id, probability, volume)
                        VALUES %s
                        RETURNING id
                    """,
                    [
                        (market_ids[i], m.probability, m.volume)
                        for i, m in enumerate(markets)
                    ],
                    fetch=True
                )
            return [str(row[0]) for row in rows]

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
