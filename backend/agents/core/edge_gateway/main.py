# edge_gateway/main.py
import uuid
import ray
from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware

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


@app.post("/ingest", response_model=IngestResponse)
async def ingest(payload: BatchIn | TransactionIn, api_key: str = Depends(require_api_key)):
    # basic rate limiting per API key
    rate_limit(api_key)

    # In Pydantic v2 → use .model_dump() instead of .dict()
    payload_dict = payload.model_dump() if hasattr(payload, "model_dump") else payload.dict()
    intent = infer_intent(payload_dict)

    if intent not in ("INGEST_BATCH", "INGEST_SINGLE"):
        raise HTTPException(status_code=400, detail="Unknown payload shape")

    request_id = str(uuid.uuid4())

    # unify to list
    items = payload.items if hasattr(payload, "items") else [payload]  # type: ignore
    # normalize (convert each model to dict safely)
    normalized = [
        normalize_record(i.model_dump() if hasattr(i, "model_dump") else i.dict())  # type: ignore
        for i in items
    ]

    # optional S3 archival of raw
    s3_uri = maybe_archive_raw(normalized[0]["merchant_id"], normalized)

    # audit start
    await app.state.db.write_audit(
        request_id=request_id,
        merchant_id=normalized[0]["merchant_id"],
        source="edge_api",
        count=len(normalized),
        status="RECEIVED",
        error=None,
    )
    log("edge.received", request_id=request_id, merchant_id=normalized[0]["merchant_id"],
        count=len(normalized), s3_uri=s3_uri)

    # parallel persistence with Ray
    obj_refs = [process_row_remote.remote(normalized)]
    results = ray.get(obj_refs)

    # Flatten Ray results (list of lists)
    all_results = [row for batch in results for row in batch]

    accepted = sum(1 for r in all_results if r.get("status") == "INGESTED")
    rejected = len(all_results) - accepted


    # audit end
    await app.state.db.write_audit(
        request_id=request_id,
        merchant_id=normalized[0]["merchant_id"],
        source="edge_api",
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
