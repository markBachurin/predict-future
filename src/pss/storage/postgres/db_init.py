import logging
import psycopg2
from pss_config.config import settings

logger = logging.getLogger(__name__)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS raw_markets (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source      VARCHAR(20)  NOT NULL,
    external_id VARCHAR(255) NOT NULL,
    ingested_at TIMESTAMPTZ  NOT NULL DEFAULT now(),
    UNIQUE (source, external_id, ingested_at)
);
CREATE INDEX IF NOT EXISTS idx_raw_markets_source    ON raw_markets (source);

CREATE TABLE IF NOT EXISTS markets (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    raw_market_id     UUID          NOT NULL REFERENCES raw_markets(id),
    source            TEXT   NOT NULL,
    external_id       VARCHAR(255)  NOT NULL,
    question          TEXT          NOT NULL,
    description       TEXT,
    category          VARCHAR(128),
    probability       NUMERIC(5,4),
    volume            NUMERIC(18,2),
    volume24hr        NUMERIC(18,2),
    price_change_day  NUMERIC(8,4),
    price_change_week NUMERIC(8,4),
    liquidity         NUMERIC(18,2),
    tags              TEXT[]        NOT NULL DEFAULT '{}',
    market_type       TEXT,
    outcomes          TEXT[] NOT NULL DEFAULT '{}',
    outcome_probabilities    NUMERIC(5,4)[],
    resolution_source        TEXT,
    ticker            TEXT,
    restricted        BOOLEAN NOT NULL DEFAULT false,
    expiry            TIMESTAMPTZ,
    is_valid          BOOLEAN       NOT NULL DEFAULT true,
    created_at        TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ   NOT NULL DEFAULT now(),
    processed         BOOLEAN       NOT NULL DEFAULT false,
    UNIQUE (source, external_id)
);
CREATE INDEX IF NOT EXISTS idx_markets_source_category ON markets (source, category);
CREATE INDEX IF NOT EXISTS idx_markets_expiry          ON markets (expiry);
CREATE INDEX IF NOT EXISTS idx_markets_is_valid        ON markets (is_valid);
CREATE INDEX IF NOT EXISTS idx_markets_volume24hr      ON markets (volume24hr);
CREATE INDEX IF NOT EXISTS idx_markets_processed       ON markets (processed);


CREATE TABLE IF NOT EXISTS llm_classifications (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    market_id           UUID         NOT NULL REFERENCES markets(id),
    is_relevant         BOOLEAN      NOT NULL DEFAULT false,
    tickers             TEXT[]       NOT NULL DEFAULT '{}',
    sectors             TEXT[]       NOT NULL DEFAULT '{}',
    direction           VARCHAR(20), -- Bullish, Bearish, Neutral
    foundational_details TEXT,
    circumstances       TEXT,
    reasoning           TEXT,
    question_filter_confidence NUMERIC(5,4),
    llm_confidence      NUMERIC(5,4),
    confidence_reason   TEXT, 
    weighted_score      NUMERIC(5,4),
    reported            BOOLEAN      NOT NULL DEFAULT false,
    classified_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
    UNIQUE (market_id)
);
CREATE INDEX IF NOT EXISTS idx_llm_classifications_market_id ON llm_classifications (market_id);
CREATE INDEX IF NOT EXISTS idx_llm_classifications_relevant ON llm_classifications (is_relevant);

CREATE TABLE IF NOT EXISTS llm_pass_results (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    market_id UUID NOT NULL REFERENCES markets(id),
    pass_number INT NOT NULL,
    is_relevant BOOLEAN,
    confidence FLOAT,
    confidence_reason TEXT,
    reason TEXT,
    created_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE (market_id, pass_number)
);
CREATE INDEX IF NOT EXISTS idx_llm_pass_results_market_id ON llm_pass_results (market_id);
CREATE INDEX IF NOT EXISTS idx_llm_pass_results_pass_number ON llm_pass_results (pass_number);
"""

def db_init() -> None:
    logger.info("Initializing database schema ...")

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
