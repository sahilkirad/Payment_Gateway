# edge_gateway/config.py
import os
from pydantic_settings import BaseSettings
from dotenv import load_dotenv

# Load .env file
load_dotenv(dotenv_path="C:/Users/DELL/SALLMA/.env")
class Settings(BaseSettings):
    # API settings
    API_TITLE: str = "Arealis Edge Gateway"
    API_VERSION: str = "v1"
    API_KEYS: str = "devkey1,devkey2"

    # CockroachDB / Postgres DSN (must be in .env)
    CRDB_DSN: str

        # S3 (optional)
    S3_ENABLE: bool = os.getenv("S3_ENABLE", "false").lower() == "true"
    S3_BUCKET: str = os.getenv("S3_BUCKET", "sallma")
    AWS_REGION: str = os.getenv("AWS_REGION", "ap-south-1")
    S3_ENDPOINT_URL: str = os.getenv("S3_ENDPOINT_URL")
   
    # Feature flags
    ARCHIVE_RAW_TO_S3: bool = os.getenv("ARCHIVE_RAW_TO_S3", "false").lower() == "true"
 
    # Rate limiting
    RATE_LIMIT_WINDOW_SEC: int = 60
    RATE_LIMIT_MAX_REQ: int = 120

    class Config:
        env_file = ".env"
        extra = "ignore"

settings = Settings()

if __name__ == "__main__":
    print(">>> DSN:", settings.CRDB_DSN)
