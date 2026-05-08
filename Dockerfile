FROM apache/airflow:2.9.0-python3.10

USER root
RUN apt-get update && apt-get install -y gcc libpq-dev && rm -rf /var/lib/apt/lists/*

USER airflow
COPY pyproject.toml .
RUN pip install --no-cache-dir -e ".[dev]" 2>/dev/null || pip install --no-cache-dir \
    pydantic>=2.0 \
    pydantic-settings>=2.0 \
    psycopg2-binary>=2.9 \
    boto3>=1.34 \
    google-genai>=1.0