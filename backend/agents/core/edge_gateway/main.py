# edge_gateway/main.py
import uuid
import ray
from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime
from .config import settings
from .auth import require_api_key
from .rate_limiter import rate_limit
from .schemas import BatchIn, TransactionIn, IngestResponse
from .payload_router import infer_intent
from .enrich import normalize_record
from .db import CockroachClient
from .s3_client import maybe_archive_raw
from .worker import process_row_remote
from .logger import log

from fastapi import APIRouter
from .s3_client import fetch_transactions

app = FastAPI(title=settings.API_TITLE, version=settings.API_VERSION)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

# Ensure Ray is available
if not ray.is_initialized():
    ray.init(ignore_reinit_error=True, include_dashboard=False)


@app.on_event("startup")
async def startup():
    """Create DB connection on service start."""
    app.state.db = CockroachClient(settings.CRDB_DSN)
    await app.state.db.connect()


@app.on_event("shutdown")
async def shutdown():
    """Close DB pool gracefully."""
    await app.state.db.close()


@app.get("/health")
def health():
    return {
        "ok": True,
        "service": settings.API_TITLE,
        "version": settings.API_VERSION,
    }

@app.get("/transactions_s3")
async def get_transactions_s3():
    try:
        data = fetch_transactions()
        return {"count": len(data), "transactions": data}
    except Exception as e:
        return {"error": str(e)}

@app.post("/ingest", response_model=IngestResponse)
async def ingest(
    payload: BatchIn | TransactionIn | None = None,
    api_key: str = Depends(require_api_key),
    from_s3: bool = False
):
    rate_limit(api_key)
    request_id = str(uuid.uuid4())

    # 1️⃣ Fetch from S3 if flagged
    if from_s3:
        normalized = fetch_transactions()  # fetch real transactions from S3
        if not normalized:
            raise HTTPException(status_code=404, detail="No transactions found in S3")

        for t in normalized:
            # ✅ type cast all string fields
            t["txn_id"] = str(t["txn_id"])
            t["merchant_id"] = str(t["merchant_id"])
            t["vendor_id"] = str(t["vendor_id"])
            t["account"] = str(t["account"])
            t["ifsc"] = str(t["ifsc"])
            t["priority"] = str(t.get("priority", "NORMAL"))
            t["geo_state"] = str(t.get("geo_state", "")) if t.get("geo_state") else None
            t["kyc_hash"] = str(t.get("kyc_hash", "")) if t.get("kyc_hash") else None
            t["currency"] = str(t.get("currency", "INR"))
            t["sla_target_sec"] = int(t.get("sla_target_sec", 60))

            # timestamp conversion
            t["timestamp"] = datetime.fromisoformat(t["timestamp"].replace("Z", "+00:00"))

        merchant_id = normalized[0]["merchant_id"]

    else:
        if payload is None:
            raise HTTPException(status_code=400, detail="Payload required if not fetching from S3")

        payload_dict = payload.model_dump() if hasattr(payload, "model_dump") else payload.dict()
        intent = infer_intent(payload_dict)
        if intent not in ("INGEST_BATCH", "INGEST_SINGLE"):
            raise HTTPException(status_code=400, detail="Unknown payload shape")

        items = payload.items if hasattr(payload, "items") else [payload]  # type: ignore
        normalized = [
            normalize_record(i.model_dump() if hasattr(i, "model_dump") else i.dict())  # type: ignore
            for i in items
        ]

        for t in normalized:
            t["txn_id"] = str(t["txn_id"])
            t["merchant_id"] = str(t["merchant_id"])
            t["vendor_id"] = str(t["vendor_id"])
            t["account"] = str(t["account"])
            t["ifsc"] = str(t["ifsc"])
            t["priority"] = str(t.get("priority", "NORMAL"))
            t["geo_state"] = str(t.get("geo_state", "")) if t.get("geo_state") else None
            t["kyc_hash"] = str(t.get("kyc_hash", "")) if t.get("kyc_hash") else None
            t["currency"] = str(t.get("currency", "INR"))
            t["sla_target_sec"] = int(t.get("sla_target_sec", 60))
            t["timestamp"] = datetime.fromisoformat(t["timestamp"].replace("Z", "+00:00"))

        merchant_id = normalized[0]["merchant_id"]
        maybe_archive_raw(merchant_id, normalized)

    # 2️⃣ Audit start
    await app.state.db.write_audit(
        request_id=request_id,
        merchant_id=merchant_id,
        source="edge_api" if not from_s3 else "s3_ingest",
        count=len(normalized),
        status="RECEIVED",
        error=None,
    )
    log("edge.received", request_id=request_id, merchant_id=merchant_id,
        count=len(normalized), s3_uri=None if not from_s3 else f"s3://{settings.S3_BUCKET}/transactions.json")

    # 3️⃣ Persist with Ray
    obj_refs = [process_row_remote.remote(normalized)]
    results = ray.get(obj_refs)
    all_results = [row for batch in results for row in batch]

    accepted = sum(1 for r in all_results if r.get("status") == "INGESTED")
    rejected = len(all_results) - accepted

    # 4️⃣ Audit end
    await app.state.db.write_audit(
        request_id=request_id,
        merchant_id=merchant_id,
        source="edge_api" if not from_s3 else "s3_ingest",
        count=len(normalized),
        status="COMPLETED" if rejected == 0 else "PARTIAL",
        error=None if rejected == 0 else f"{rejected} failed",
    )
    log("edge.persisted", request_id=request_id, accepted=accepted, rejected=rejected)

    return IngestResponse(
        request_id=request_id,
        received=len(normalized),
        accepted=accepted,
        rejected=rejected,
    )
