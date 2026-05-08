import json
from src.pss.storage.postgres.client import PostgresClient

client = PostgresClient()

with client._get_connection() as conn:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 
                m.question,
                m.description,
                lc.direction,
                lc.llm_confidence,
                lc.weighted_score,
                lc.reasoning,
                lc.foundational_details,
                lc.confidence_reason,
                m.probability
            FROM markets m
            INNER JOIN llm_pass_results lpr ON m.id = lpr.market_id
            INNER JOIN llm_classifications lc ON m.id = lc.market_id
            WHERE lpr.pass_number = 2
                AND lpr.is_relevant = true  -- Assuming "passed" means is_relevant = true
                AND m.is_valid = true;      -- Optional: only get valid markets
        """)
        rows = cur.fetchall()
        for row in rows:
            print(f"Question: {row[0]}")
            print(f"Descr: {row[1][:300]}")
            print(f"probability of event: {row[-1]}")
            print(f"Direction: {row[2]}...")
            print(f"llm_confidence : {row[3]}")
            print(f"weighted_score: {row[4]}")
            print(f"reasoning: {row[5]}")
            print(f"foundational details: {row[6]}")
            print(f"confidence reason: {row[7]}")
            print("-" * 150)