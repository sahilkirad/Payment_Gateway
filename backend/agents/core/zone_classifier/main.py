# backend/agents/zone_classifier/main.py
import ray
import asyncio
from backend.data_services import CockroachDBClient
from backend.agents.zone_classifier.bank_affinity import get_zone

async def core_logic(transaction: dict) -> dict:
    print("▶️ Running Zone Classifier Agent...")
    txn_id = transaction.get("txn_id")
    ifsc = transaction.get("ifsc", "")
    zone = get_zone(ifsc)

    return {
        "status": "success",
        "txn_id": txn_id,
        "zone": zone
    }

@ray.remote
def run(transaction: dict) -> dict:
    async def _inner():
        db = CockroachDBClient()
        result = await core_logic(transaction)
        if result["status"] == "success":
            await db.save_agent_result("zone_classifier", result["txn_id"], result)
        return result
    return asyncio.run(_inner())
