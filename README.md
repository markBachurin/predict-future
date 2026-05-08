# PSS - Polymarket Signal System

The Polymarket Signal System (PSS) is an automated intelligence pipeline designed to distill actionable insights from prediction markets. It leverages a two-pass LLM architecture to identify, classify, and score market events relevant to investment portfolios.

## Key Features

- **Two-Pass LLM Classification:** Efficiently filters and analyzes market data using Google Gemini.
- **Automated Ingestion:** Real-time data fetching from Polymarket with multi-stage filtering.
- **Weighted Scoring:** A deterministic model combining qualitative LLM insights with quantitative market metrics.
- **Airflow Orchestration:** Robust pipeline management and monitoring.

## Prerequisites

- **Docker & Docker Compose**
- **Environment Variables:** Create a `.env` file in the root directory. Essential variables include:
    - **Database:** `PG_USER`, `PG_PASSWORD`, `PG_DB`
    - **Airflow:** `AIRFLOW_ADMIN`, `AIRFLOW_ADMIN_PASSWORD`, `AIRFLOW_WEBSERVER_SECRET_KEY`
    - **Polymarket:** `POLYMARKET_BASE_URL`, `POLYMARKET_VOLUME_MIN`, etc.
    - **AWS:** `S3_BUCKET`, `AWS_ACCESS_KEY`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION_NAME`
    - **LLM:** `GEMINI_API_KEY`
    - **Pipeline:** `INGESTION_INTERVAL_MINUTES`

## Launch Instructions

To launch the project, execute the following commands in order:

```bash
docker compose down
docker compose build --no-cache
docker compose run --rm airflow-init
docker compose up -d airflow-webserver airflow-scheduler
```

Once the containers are running, you can access the Airflow UI at `http://localhost:8080` using the credentials defined in your `.env` file (`AIRFLOW_ADMIN` / `AIRFLOW_ADMIN_PASSWORD`).

## Architecture

For a detailed deep dive into the system design, data flow, and scoring models, please refer to [ARCHITECTURE.md](./ARCHITECTURE.md).
