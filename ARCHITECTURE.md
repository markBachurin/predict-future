# PSS - Polymarket Signal System
### BIT Capital Internal Internship Case Study Architecture Document

---

## 1. Summary

The Polymarket Signal System (PSS) is an automated intelligence pipeline designed to distill actionable insights from the global prediction market landscape for BIT Capital. It ingests real-time market data, filters for relevance to our portfolio, and uses a complex two-pass Large Language Model (LLM) architecture to classify and score market events. PSS proactively identifies potential market-moving events, translating complex market dynamics into weighted signals that enable analysts to make informed, data-driven decisions and maintain a competitive edge.

---

## 2. System Design Principles

*   **Two-Pass LLM Classification:** Uses a staged LLM evaluation to lower API costs and improve classification precision. The initial pass acts as a strict gatekeeper, rapidly filtering irrelevant markets, while the second pass performs deep causal analysis on a refined subset.
*   **Deterministic Signal Generation:** LLM inferences for classification and scoring are performed with a temperature of 0.0, ensuring reproducibility and consistency of market analysis. This minimizes stochasticity in signal generation.
*   **Comprehensive Audit Trail:** All intermediate LLM classification results (Pass 1 and Pass 2) are saved, providing full transparency and traceability for every analytical decision made by the system.
*   **Deterministic Data Processing:** Ingestion and classification processes are designed to be deterministic, enabling safe re-runs without adverse effects. `ON CONFLICT DO UPDATE` strategies prevent data duplication and ensure data freshness after each ingestion cycle.
---

## 3. Data Flow Diagram

```mermaid
graph TD
    A[Polymarket API] -> B{Ingestion Layer filters by volume, liquidity, volume24hr, category, 
                        tags + prince of center of gravity for events with multiple markets inside
                        + archive raw data to S3 for potential future data recovery};
                        
    B -> C{Data Validation: RawMarket -> ValidatedMarket}
    
    C -> D{PostgreSQL: saving of raw_market and validated_market into to Supabase 
                        (marked unprocessed)};
                        
    C -> D{Airflow DAG: pss_classify_markets};
    
    D -> E{SELECT unprocessed markets};
    
    E -> F{LLM Pass 1: Question Filter (Batched LLM API call)};
    
    G -> H{Filter relevant (confidence > 0.7, save record of relevance to llm_pass_results)};
    
    H -> I{LLM Pass 2: Description Reasoning (Batch 5)};
    
    I -> J{PostgreSQL: llm_pass_results (Pass 2)};
    
    J -> K{Weighted Score Calculation};
    
    K -> L{PostgreSQL: llm_classifications};
    
    L -> M{Mark all initial raw_markets as processed};
    
    M -> N[PDF Report Generation];

    %% Volume Annotations
    subgraph Volumes
        A - "~30_000-40_000 markets/cycle" -> B - "50 markets remain";
        B - "~50 markets" -> C;
        C - "~50 markets/cycle" -> E - "20-30 markets remain";
        E - "~20-30 markets survive" -> F;
        H - "~20-30 classifications" -> I;
    end
```

---

## 4. Pipeline Stages

### 4.1. Ingestion Layer

*   **Purpose:** To fetch raw prediction market data from external API, apply initial locally executable filters, archive markets to S3, and saves it into database.
*   **Input / Output:**
    *   Input: Raw market data from Polymarket API (JSON).
    *   Output: `raw_markets` and `markets` records in PostgreSQL.
*   **Key Logic:**
    *   Connects to Polymarket API.
    *   Applies numeric filters based on `volume`, `volume24hr`, and `liquidity` thresholds defined in configuration.
    *   Performs category-base filtering: filters away all markets that have NOISE or EXCLUDED category
    *   Performs tag-based filtering: cleans tags, intersects with `BIT_CAPITAL_HOLDINGS['relevant_tags']`, and a given market ramains if 50% + of its tags are relevant.
    *   Parses filtered data into `RawMarket` objects.
    *   Inserts new raw markets into `raw_markets` table, marking `processed=false`.
    *   Upserts latest market data into the `markets` table (`ON CONFLICT DO UPDATE`).
*   **Design Decisions and Tradeoffs:**
    *   **Early Filtering:** Initial numeric, category and tag filters reduce the volume of data passed to subsequent stages, lowering processing costs and database load. This risks false negatives for nascent but potentially relevant markets.
    *   **Deterministic Upserts:** `ON CONFLICT DO UPDATE` ensures that repeated ingestion of the same market updates existing records, maintaining data freshness without creating duplicates.
    *   **Separate `raw_markets`:** Stores the external_id and ingestion timestamp.

### 4.2. Classification DAG

*   **Purpose:** Orchestrates the LLM classification pipeline.
*   **Input / Output:**
    *   Input: Unprocessed markets from PostgreSQL `markets` table + markets to be re-classified that were marked as relevant in previous classification sessions.
    *   Output: LLM classification results (`llm_pass_results`, `llm_classifications`) saved to PostgreSQL.
*   **Key Logic:**
    *   Runs on a `ingestion_interval_minutes` schedule (defaulting to 60 minutes).
    *   Executes `ensure_db_schema` task to verify database integrity.
    *   Invokes `classify_markets` task.
    *   Overrides `MarketClassifier` semaphores to 1, forcing sequential LLM calls.
*   **Design Decisions and Tradeoffs:**
    *   **Sequential LLM Calls (Current):** DAG explicitly enforces sequential LLM calls. This prioritizes stability and simplifies error handling during initial deployment + lower chance or rate limiting.

### 4.3. LLM Pass 1 - Question Filter

*   **Purpose:** To rapidly identify markets highly relevant to BIT Capital's investment focus based solely on the market question and associated tags, acting as a strict gatekeeper for further analysis.
*   **Input / Output:**
    *   Input: `market_id`, `question`, `tags` for a batch of markets.
    *   Output: JSON object per market containing `market_id`, `is_relevant`, `confidence`, `reason`, `confidence_reason`. Saved to `llm_pass_results` (pass_number=1).
*   **Key Logic:**
    *   Processes markets in batches (default 10) to lower API calls.
    *   Uses `QUESTION_SYSTEM_PROMPT` to guide the LLM to apply strict relevance criteria: matching a held ticker, a focus sector, an exact macro theme, or multiple relevant signal tags.
    *   Filters out markets where `is_relevant` is false or `confidence` is below 0.7.
*   **Design Decisions and Tradeoffs:**
    *   **Aggressive Filtering:** Focuses on high-precision filtering to minimize downstream processing for irrelevant markets. 
    *   **Context Efficiency:** Feeds minimal market data (question, tags) to the LLM to reduce token usage for the first pass.
    *   **Batch Processing:** Groups multiple market questions into a single LLM API call to reduce overhead, with a batch size of 10.

### 4.4. LLM Pass 2 - Description Reasoning

*   **Purpose:** To perform in-depth analysis on markets that survived Pass 1, mapping markets to specific tickers and sectors, assessing directional impact (bullish/bearish/neutral), and evaluating signal urgency.
*   **Input / Output:**
    *   Input: `market_id`, `question`, `description`, `probability`, `liquidity`, `price_change_day`, `price_change_week` for a batch of markets.
    *   Output: JSON object per market containing `market_id`, `tickers`, `sectors`, `direction`, `llm_confidence`, `confidence_reason`, `foundational_details`, `circumstances`, `reasoning`. Saved to `llm_pass_results` (pass_number=2).
*   **Key Logic:**
    *   Processes markets in batches (default 5).
    *   Uses `DESCRIPTION_SYSTEM_PROMPT` to instruct the LLM to act as a senior investment analyst, tracing causal chains, assessing urgency from market dynamics, and providing potential direction of price for relevant items.
    *   Generates detailed qualitative analysis.
*   **Design Decisions and Tradeoffs:**
    *   **Deep Analysis:** Provides the LLM with all market data (including description and price dynamics) to allow nuanced analysis. This increases token usage per market but is justified by the higher value of these highly relevant markets.
    *   **Directional Commitment:** Prompts the LLM to explicitly state a directional impact (bullish/bearish/neutral), forcing a clear signal for analysts.
    *   **Limited Batching:** Processes fewer markets per batch (5) compared to Pass 1, allowing the LLM more context per market without exceeding token limits for detailed reasoning.

### 4.5. Weighted Score Calculation

*   **Purpose:** To put LLM confidence and numeric market metrics into a single, standardized numerical signal score, providing a ranked prioritization for analyst review.
*   **Input / Output:**
    *   Input: LLM Pass 2 results and raw market data (`volume`, `volume24hr`, `liquidity`, `price_change_day`, `price_change_week`).
    *   Output: A floating-point `weighted_score` for each classified market.
*   **Key Logic:**
    *   Applies a weighted sum of normalized metrics:
        *   `llm_confidence` (0.30 weight)
        *   Log-normalized `volume` (0.20 weight)
        *   Log-normalized `volume24hr` (0.15 weight)
        *   Log-normalized `liquidity` (0.15 weight)
        *   Normalized absolute `price_change_day` (0.12 weight, capped at 0.1)
        *   Normalized absolute `price_change_week` (0.08 weight, capped at 0.15)
    *   Numeric metrics (`volume`, `volume24hr`, `liquidity`) are log-normalized to a 0-1 scale using empirically derived minimum and maximum values (`volume`: log range [10.83, 16.77]; `volume24hr` & `liquidity`: log range [8.53, 13.24]).
    *   Price changes are normalized by capping them at specified thresholds before scaling to 0-1.
*   **Design Decisions and Tradeoffs:**
    *   **Hybrid Scoring:** Combines qualitative LLM insights with numeric market data, leveraging the strengths of both to create a reliable signal.
    *   **Log Normalization:** Addresses the skewed distribution of market volume and liquidity metrics, preventing outliers from disproportionately influencing the score.
    *   **Tunable Weights:** Allows for adjustment of component importance based on empirical performance or strategic shifts.

### 4.6. Storage

*   **Purpose:** To store the final market classifications and update the processing status of all ingested markets.
*   **Input / Output:**
    *   Input: Final `llm_classifications` data.
    *   Output: Records in `llm_classifications` table, updated `processed` status in `markets` table.
*   **Key Logic:**
    *   Inserts final classifications into the `llm_classifications` table, supporting reclassification via `ON CONFLICT DO UPDATE`.
    *   Updates the `processed` flag to `true` for all markets that entered the classification pipeline, regardless of whether they resulted in a final classification. This prevents reprocessing of already evaluated markets.
*   **Design Decisions and Tradeoffs:**
    *   **Deterministic Classification Storage:** `ON CONFLICT DO UPDATE` allows for re-running classification on previously analyzed markets, ensuring that the latest and most accurate LLM insights are always reflected.
    *   **"Mark Processed" Strategy:** Ensures that all markets are eventually marked as processed, even if they don't yield a final classification. This prevents an accumulation of perpetually unprocessed markets, trading off potential re-analysis of failed markets for pipeline flow.

---

## 5. LLM Classification Architecture

The PSS uses a two-pass LLM architecture using `gemini-2.5-flash` to efficiently and accurately identify investment signals. This staged approach is critical for balancing computational API cost with analytical depth.

### 5.1. Design Philosophy: Progressive Refinement

The two-pass design uses a progressive refinement philosophy:
1.  **Broad Local Sweep (Numerical or Locally Executable filtering):** Quickly sifts through a large volume of raw markets, through numeric metrics or categories and tags  
2.  **Broad LLM Sweep (Pass 1):** Relatively quick, cost-efficient and strict, high-level relevance criteria. This pass prioritizes efficiency and tries to eliminate the vast majority of irrelevant data early.
2.  **Deep Dive (Pass 2):** Concentrates LLM API resources on the highly relevant subset identified by Pass 1, performing detailed causal analysis and signal extraction. This pass prioritizes analytical depth and accuracy.

This architecture lowers LLM token usage and API costs by avoiding expensive, detailed analysis on markets unlikely to yield actionable information.

### 5.2. Pass 1: Question Filter Strategy

*   **Purpose:** Act as a front-line gatekeeper, filtering markets based on their immediate textual content (question and tags) against BIT Capital's core holdings and focus areas.
*   **Prompt Strategy (`QUESTION_SYSTEM_PROMPT`):**
    *   **Role-Play:** Instructs the LLM to act as a "gatekeeper" with a mandate for aggressive filtering.
    *   **Explicit Criteria:** Provides three precise, easy-to-evaluate criteria (ticker match, sector match, exact macro theme match). This significantly reduces LLM hallucination and ensures focused evaluation.
    *   **Confidence Guidance:** Defines a confidence scoring guide to ensure consistent and justifiable confidence levels.
    *   **Strict Formatting:** Enforces raw JSON array output to facilitate programmatic parsing.


### 5.3. Pass 2: Description Reasoning Strategy

*   **Purpose:** To conduct in-depth investment analysis on markets marked relevant by Pass 1, by analysing complex event descriptions for structured investment signals.
*   **Prompt Strategy (`DESCRIPTION_SYSTEM_PROMPT`):**
    *   **Role-Play:** Sets LLM's role to a "Senior Investment Analyst" who is analyzing an already relevant market.
    *   **Comprehensive Context:** Provides extensive background on BIT Capital's holdings, sectors, and macro themes, including detailed descriptions of tickers.
    *   **Causal Chain Emphasis:** Explicitly instructs the LLM to trace causal chains from market events to specific tickers and sectors.
    *   **Directional & Urgency Assessment:** Guides the LLM to determine the directional impact (bullish/bearish/neutral) and assess urgency based on probability and price changes.
    *   **Confidence Guidance:** Offers a detailed confidence scoring guide.
    *   **Strict Formatting:** Enforces raw JSON array output.

---

## 6. Scoring Model

The `weighted_score` provides a normalized, numeric measure of a market's potential investment signal strength. It combines LLM confidence with other numeric market metrics. The total score is a sum of its weighted components, normalized to a 0-1 scale.

**Formula:**

```
score = (llm_confidence * 0.30) 
      + (normalized_volume * 0.20) 
      + (normalized_volume24hr * 0.15) 
      + (normalized_liquidity * 0.15) 
      + (normalized_abs_price_change_day * 0.12) 
      + (normalized_abs_price_change_week * 0.08)
```

**Component Breakdown:**

*   **`llm_confidence` (Weight: 0.30):**
    *   **Measures:** The LLM's self-assessed certainty in its Pass 2 analysis, reflecting the clarity of the causal chain from the market event to BIT Capital's portfolio.
    *   **Rationale:** This is the most heavily weighted component, because of the LLM's ability to provide qualitative information and trace complex connections that numeric metrics alone cannot capture.

*   **`normalized_volume` (Weight: 0.20):**
    *   **Measures:** The total trading volume in the prediction market.
    *   **Rationale:** High volume shows market's interest and belief into. Log-normalization handles power-law distribution of market volumes.

*   **`normalized_volume24hr` (Weight: 0.15):**
    *   **Measures:** Trading volume over the last 24 hours.
    *   **Rationale:** Recent activity and engagement, indicats current active or breaking market event. Log-normalization applied.

*   **`normalized_liquidity` (Weight: 0.15):**
    *   **Measures:** The total available capital in the market.
    *   **Rationale:** Higher liquidity implies a more mature and trusted market, where larger positions can be taken without significant price impact, thus a more reliable signal. Log-normalization applied.

*   **`normalized_abs_price_change_day` (Weight: 0.12):**
    *   **Measures:** The abs probability change over the last 24 hours, capped at 0.1.
    *   **Rationale:** Significant recent price movement indicates that market participants are rapidly updating their beliefs, suggesting an urgent and developing situation. The cap prevents extreme, possibly anomalous, movements from distorting the score.

*   **`normalized_abs_price_change_week` (Weight: 0.08):**
    *   **Measures:** The abs probability change over the last 7 days, capped at 0.15.
    *   **Rationale:** Provides a broader trend perspective. Sustained movement over a week reinforces the signal's reliability, while the lower weight compared to 24hr change removes bias of older information.

All numeric normalization make sure that metrics contribute proportionally to the final score, preventing any single extreme value from dominating.

---

## 7. Database Design

The PSS uses a PostgreSQL database to store raw market data, intermediate LLM results, and final classifications hosted on Supabase. 

### 7.1. Table Purposes and Relationships

*   **`raw_markets`**
    *   **Purpose:** Stores external id, market_id, ingestion timestamp. Acts as a staging area of all ingested events.
    *   **Relationship:** One-to-one with `markets` (via `external_id` & `source` uniqueness, `id` to `raw_market_id`).
    *   **Key Fields:** `id` (PK), `source`, `external_id`, `ingested_at`.

*   **`markets`**
    *   **Purpose:** Stores a normalized, de-duplicated, and enriched view of prediction market data, ready for classification. This table is continuously updated with the latest market state.
    *   **Relationship:** One-to-one with `raw_markets` (via `raw_market_id` FK). One-to-many with `llm_pass_results`, one-to-one with `llm_classifications` (via `id` PK).
    *   **Key Fields:** `id` (PK), `raw_market_id` (FK), `question`, `description`, `category`, `probability`, `volume`, `liquidity`, `tags[]`, `outcomes[]`, `processed` (flag for classification status), etc.

*   **`llm_pass_results`**
    *   **Purpose:** Stores the fields from each LLM classification pass (Pass 1 and Pass 2). Important for auditing, debugging, and understanding LLM reasoning.
    *   **Relationship:** Many-to-one with `markets` (via `market_id` FK).
    *   **Key Fields:** `market_id` (FK), `pass_number` (1 or 2), `is_relevant`, `confidence`, `confidence_reason`, `reason`, `created_at`.

*   **`llm_classifications`**
    *   **Purpose:** Stores the final, consolidated, and scored investment signals derived from the LLM pipeline, serving as the final output for analyst review.
    *   **Relationship:** One-to-one with `markets` (via `market_id` FK).
    *   **Key Fields:** `market_id` (FK), `is_relevant`, `tickers[]`, `sectors[]`, `direction`, `llm_confidence`, `weighted_score`, `foundational_details`, `circumstances`, `reasoning`, `classified_at`.

---

## 8. Failure Handling & Resilience

The PSS is designed with several mechanisms to ensure robustness and graceful degradation in the face of various failures.

*   **LLM API Failures:**
    *   **Retry Mechanism:** The `GeminiAPIClient` implements a retry strategy (3 retries with a 150-second delay) for transient API errors or JSON parsing failures.
    *   **Logging:** Detailed error logging (warnings for retries, errors for ultimate failures) provides visibility into LLM interaction issues.

*   **Database Failures:**
    *   **Connection Management:** Database connections are managed via `contextmanager` in `PostgresClient`, ensuring connections are correctly closed and transactions are rolled back on exceptions.
    *   **Deterministic Writes:** `ON CONFLICT DO UPDATE` statements for `raw_markets`, `markets`, and `llm_classifications` prevent data corruption or duplication during partial failures and re-runs.

*   **Quota Exhaustion:**
    *   If LLM API quotas are exhausted, the `GeminiAPIClient` will raise an exception after retries. This will propagate up and cause the Airflow task to fail, triggering alerts for manual intervention and quota review.

*   **Partial Batch Failures (LLM):**
    *   **Pass 1:** If a market within a Pass 1 batch fails LLM processing (e.g., malformed response), the entire is excluded from the results map, preventing it from proceeding to Pass 2. 
    *   **Pass 2:** Similar handling applies; individual market analysis failures in Pass 2 (e.g., missing `market_id` in response) prevent that specific market from being classified, but other markets in the batch continue to be processed.

*   **Airflow Task Failures:**
    *   **Retries:** The DAG's `DAG_DEFAULT_ARGS` define retries (`retries: 0`, `retry_delay: 2 minutes`) for tasks. Currently, retries are disabled, meaning any task failure will result in an immediate DAG failure and alert. This ensures rapid identification of issues but requires prompt manual intervention.

---

## 9. Configuration Reference (`pss_config/config.py`)

All core operational parameters are managed through the `Settings` class, loaded from environment variables (`.env` file).

| Setting                       | Purpose                                                              | Default (if applicable)     |
| :---------------------------- | :------------------------------------------------------------------- | :-------------------------- |
| `db_host`                     | PostgreSQL database host address                                     |                             |
| `db_port`                     | PostgreSQL database port                                             |                             |
| `db`                          | PostgreSQL database name                                             |                             |
| `db_user`                     | PostgreSQL database user                                             |                             |
| `db_password`                 | PostgreSQL database password                                         |                             |
| `polymarket_base_url`         | Base URL for the Polymarket API                                      |                             |
| `polymarket_volume_min`       | Minimum total volume threshold for market ingestion                  |                             |
| `polymarket_page_limit`       | Page size limit for fetching markets from Polymarket API             |                             |
| `polymarket_volume24hr_min`   | Minimum 24-hour volume threshold for market ingestion                |                             |
| `polymarket_liquidity_min`    | Minimum liquidity threshold for market ingestion                     |                             |
| `kalshi_base_url`             | Base URL for the Kalshi API (currently unused)                       |                             |
| `s3_bucket`                   | AWS S3 bucket name for historical storage                            |                             |
| `aws_access_key`              | AWS Access Key ID                                                    |                             |
| `aws_secret_access_key`       | AWS Secret Access Key                                                |                             |
| `aws_region_name`             | AWS region for S3 operations                                         |                             |
| `gemini_api_key`              | Google Gemini API key                                                |                             |
| `llm_model`                   | LLM model identifier (e.g., `gemini-2.5-flash`)                      | `gemini-2.5-flash`          |
| `llm_temperature`             | LLM generation temperature (0.0 for deterministic output)            | `0.0`                       |
| `question_thread_limit`       | Configured max concurrent threads for LLM Pass 1 (overridden by DAG) | `5`                         |
| `description_thread_limit`    | Configured max concurrent threads for LLM Pass 2 (overridden by DAG) | `5`                         |
| `ingestion_interval_minutes`  | Interval in minutes for the Airflow DAG schedule                     |                             |
| `expiry_max_days`             | Maximum market expiry in days to consider for ingestion              | `180`                       |
| `batch_size`                  | General batch size for data processing (e.g., ingestion)             | `2000`                      |

---

## 10. Filter Funnel

The following diagram illustrates the market reduction at each stage of the PSS pipeline, with approximate volumes based on observed data.

```
Polymarket API           
      ↓
      ↓  ~40_000-30_000 markets/cycle
      ↓
      ↓  Category filter
      ↓
      ↓ ~ 2000 - 3000 markets
      ↓
      ↓ Numeric filters (volume, volume24hr, liquidity)
      ↓
      ↓ ~ 500 markets
      ↓
      ↓ Tag filter (50% relevant tags)
      ↓
      ↓ ~50 markets
      ↓
      ↓ LLM Pass 1 (Question Filter, confidence > 0.7)
      ↓ (Batch size: 10 per LLM call, currently sequential)
      ↓
      ↓  ~5-15 markets
      ↓
      ↓ LLM Pass 2 (Description Reasoning)
      ↓ (Batch size: 5 per LLM call, currently sequential)
      ↓
      ↓  ~3-10 classifications
      ↓
      ↓ Weighted Score Calculation
      ↓
   Final Signals
```

---
