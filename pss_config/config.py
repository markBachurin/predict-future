from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path
from datetime import timedelta

DAG_DEFAULT_ARGS={
        "owner" : "pss",
        "retries": 0,
        "retry_delay": timedelta(minutes=2),
    }

class Settings(BaseSettings):
    # db supabase
    db_host: str
    db_port: int
    db: str
    db_user: str
    db_password: str

    # Polymarket
    polymarket_base_url: str
    polymarket_volume_min: float
    polymarket_page_limit: int
    polymarket_volume24hr_min: float
    polymarket_liquidity_min: float

    # Kalshi
    kalshi_base_url: str

    s3_bucket: str
    aws_access_key: str
    aws_secret_access_key: str
    aws_region_name: str

    # LLM
    gemini_api_key: str
    llm_model : str = "gemini-2.5-flash"
    llm_temperature : float = 0.0
    gatekeep_thread_limit : int = 10
    reason_thread_limit : int = 10

    #p pipeline
    ingestion_interval_minutes: int
    expiry_max_days: int = 180
    batch_size: int = 2000


    model_config = SettingsConfigDict(
        env_file = Path(__file__).parent.parent / ".env",
        env_file_encoding = "utf-8",
        extra="ignore",
    )

settings = Settings()
