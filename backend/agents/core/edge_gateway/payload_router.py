# edge_gateway/payload_router.py
from typing import Literal, Any, Dict

Intent = Literal["INGEST_BATCH", "INGEST_SINGLE", "UNKNOWN"]

def infer_intent(body: Any) -> Intent:
    if isinstance(body, dict) and "txn_id" in body:
        return "INGEST_SINGLE"
    if isinstance(body, dict) and "items" in body and isinstance(body["items"], list):
        return "INGEST_BATCH"
    if isinstance(body, list):
        return "INGEST_BATCH"
    return "UNKNOWN"
