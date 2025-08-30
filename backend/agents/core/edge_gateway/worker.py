# worker.py
import asyncio
import ray
from typing import List, Dict, Any
from .db import CockroachClient
from .config import settings

async def _persist_batch(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    client = CockroachClient(settings.CRDB_DSN)
    await client.connect()
    try:
        await client.upsert_transactions(rows)
        return [{"txn_id": r["txn_id"], "status": "INGESTED"} for r in rows]
    finally:
        await client.close()

@ray.remote
def process_row_remote(rows: List[dict]):
    return asyncio.run(_persist_batch(rows))

