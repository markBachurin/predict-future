import psycopg2
import psycopg2.extras
from contextlib import contextmanager
from config.config import settings
from src.pss.ingestion.base import RawMarket

def get_connection():
    return psycopg2.connect(settings.database_url)

@contextmanager
def get_conn():
    conn = get_connection()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def upsert_raw_market(conn, market: RawMarket) -> str | None:
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO raw_markets (source, external_id, raw_payload)
            VALUES (%s, %s, %s)
            ON CONFLICT (source, external_id, ingested_at)  DO NOTHING
            RETURN id
        """, (market.source, market.external_id, psycopg2.extras.Json(market.raw_payload)))

        row = cur.fetchone()
        return str(row[0]) if row else None

def upsert_market(conn, raw_id, market:RawMarket, is_valid: bool) -> str:
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
        """,(
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
        return str(conn.cursor().fetchone()[0])

def insert_snapshot(conn, market_id: str, market: RawMarket):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO market_snapshots (market_id, probability, volume)
            VALUES (%s, %s, %s)
        """, (market_id, market.probability, market.volume))