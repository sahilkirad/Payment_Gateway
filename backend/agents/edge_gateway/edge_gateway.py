# backend/agents/edge_gateway/edge_gateway.py
import ray
import asyncio
from backend.data_services import CockroachDBClient
from backend.agents.edge_gateway.auth import validate_auth
from backend.agents.edge_gateway.rate_limiter import check_rate_limit
from backend.agents.edge_gateway.enrich import enrich_payouts
from backend.agents.edge_gateway.config import GATEWAY_CONFIG

async def core_logic(request: dict) -> dict:
    print("▶️ Running Edge Gateway Agent...")

    headers = request.get("headers", {})
    payouts = request.get("body", [])

    merchant_id = headers.get("x-merchant-id", "default")

    # Step 1: Auth
    ok, msg = validate_auth(headers)
    if not ok:
        return {"status": "failed", "reason": msg}

    # Step 2: Rate limiting
    ok, msg = check_rate_limit(GATEWAY_CONFIG["max_requests_per_minute"], merchant_id)
    if not ok:
        return {"status": "failed", "reason": msg}

    # Step 3: Enrich
    enriched = enrich_payouts(payouts)

    return {"status": "success", "normalized_batch": enriched}

@ray.remote
def run(request: dict) -> dict:
    async def _inner():
        db = CockroachDBClient()
        result = await core_logic(request)
        if result["status"] == "success":
            for txn in result["normalized_batch"]:
                await db.save_agent_result("edge_gateway", txn["txn_id"], txn)
        return result
    return asyncio.run(_inner())
