from typing import Optional
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # API
    API_TITLE: str = "Arealis Edge Gateway"
    API_VERSION: str = "v1"
    API_KEYS: str = "devkey1,devkey2"  # comma-separated keys for quick start

    # CockroachDB (Postgres wire)
    CRDB_DSN: Optional[str] = None  # Will be loaded from DATABASE_URL in .env

    # S3 (optional)
    S3_ENABLE: bool = False
    S3_BUCKET: str = "sallma"
    AWS_REGION: str = "ap-south-1"
    S3_ENDPOINT_URL: Optional[str] = None

    # Feature flags
    ARCHIVE_RAW_TO_S3: bool = False

    # Rate limiting
    RATE_LIMIT_WINDOW_SEC: int = 60
    RATE_LIMIT_MAX_REQ: int = 120

    class Config:
        case_sensitive = True
        env_file = ".env"

settings = Settings()