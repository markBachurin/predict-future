import logging
import psycopg2
from pss_config.config import settings

logger = logging.getLogger(__name__)

SCHEMA_SQL="""
CREATE TABLE IF NOT EXISTS raw_markets (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source      VARCHAR(20)  NOT NULL,
    external_id VARCHAR(255) NOT NULL,
    processed   BOOLEAN      NOT NULL DEFAULT false,
    ingested_at TIMESTAMPTZ  NOT NULL DEFAULT now(),
    UNIQUE (source, external_id, ingested_at)
);
CREATE INDEX IF NOT EXISTS idx_raw_markets_processed ON raw_markets (processed, ingested_at);
CREATE INDEX IF NOT EXISTS idx_raw_markets_source    ON raw_markets (source);

CREATE TABLE IF NOT EXISTS markets (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    raw_market_id UUID          NOT NULL REFERENCES raw_markets(id),
    source        VARCHAR(20)   NOT NULL,
    external_id   VARCHAR(255)  NOT NULL,
    question      TEXT          NOT NULL,
    category      VARCHAR(100),
    probability   NUMERIC(5,4)  NOT NULL,
    volume        NUMERIC(18,2),
    expiry        TIMESTAMPTZ,
    is_valid      BOOLEAN       NOT NULL DEFAULT true,
    created_at    TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ   NOT NULL DEFAULT now(),
    UNIQUE (source, external_id)
);
CREATE INDEX IF NOT EXISTS idx_markets_source_category ON markets (source, category);
CREATE INDEX IF NOT EXISTS idx_markets_expiry          ON markets (expiry);
CREATE INDEX IF NOT EXISTS idx_markets_is_valid        ON markets (is_valid);

CREATE TABLE IF NOT EXISTS market_snapshots (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    market_id   UUID         NOT NULL REFERENCES markets(id),
    probability NUMERIC(5,4) NOT NULL,
    volume      NUMERIC(18,2),
    recorded_at TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_snapshots_market_time ON market_snapshots (market_id, recorded_at);
"""

def db_init() -> None:
    logger.info("Initializing database scheama ...")

    conn = psycopg2.connect(
        host=settings.db_host,
        port=settings.db_port,
        dbname=settings.db,
        user=settings.db_user,
        password=settings.db_password,
        connect_timeout=10,
        sslmode="require",
    )

    try:
        with conn.cursor() as cur:
            cur.execute(SCHEMA_SQL)
        conn.commit()
        logger.info("Schema initialised successfully.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Schema init failed: {e}")
        raise
    finally:
        conn.close()
