# edge_gateway/s3_client.py
import json, uuid
from typing import Any, Dict, List
from .config import settings

try:
    import boto3
    _HAS_BOTO = True
except Exception:
    _HAS_BOTO = False

def maybe_archive_raw(merchant_id: str, items: List[Dict[str, Any]]) -> str | None:
    if not settings.ARCHIVE_RAW_TO_S3:
        return None
    if not _HAS_BOTO:
        return None
    key = f"incoming/{merchant_id}/{uuid.uuid4()}.json"
    s3 = boto3.client("s3", region_name=settings.AWS_REGION)
    s3.put_object(
        Bucket=settings.S3_BUCKET,
        Key=key,
        Body=json.dumps(items, default=str).encode("utf-8"),
        ContentType="application/json"
    )
    return f"s3://{settings.S3_BUCKET}/{key}"
