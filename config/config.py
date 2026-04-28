from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path

class Settings(BaseSettings):
    database_url: str

    # Polymarket
    polymarket_base_url: str
    polymarket_volume_min: float
    polymarket_page_limit: int

    # Kalshi
    kalshi_base_url: str
    kalshi_api_key: str

    s3_bucket: str
    aws_endpoint_url : str

    # LLM
    anthropic_api_key: str

    #p pipeline
    ingestion_interval_minutes: int = 15
    expiry_max_days: int = 100 # ignore markets expiring beyond 6 months

    region_name: str

    model_config = SettingsConfigDict(
        env_file = Path(__file__).parent.parent / ".env",
        env_file_encoding = "utf-8"
    )

settings = Settings()
