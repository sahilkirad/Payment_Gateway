import ray
from typing import Dict, Any
from backend.data_services.cockroach_client import db_client

@ray.remote
class ControllerAgent:
    def __init__(self):
        # The agent doesn't need its own connection logic. It will use the shared client.
        print("✅ ControllerAgent initialized.")

    async def adjudicate_and_store(self, transaction_state: Dict[str, Any]) -> Dict[str, Any]:
        """
        This is the primary method. It performs two critical functions:
        1. Adjudicates the final outcome based on the input state.
        2. Persists the entire final state to CockroachDB.
        """
        txn_id = transaction_state.get("txn_id", "Unknown")
        print(f"▶️ CONTROLLER: Adjudicating transaction {txn_id}")

        # --- Dynamic Adjudication Logic ---
        dispatch_status = transaction_state.get("status")
        latency_ms = transaction_state.get("latency_ms")
        sla_deadline_ms = 300000  # This would be looked up from sla_id in a real system

        final_status = "FAILED_UNKNOWN"
        if dispatch_status in ["SUCCESS", "COMPLETED"]:
            final_status = "COMPLETED_SUCCESSFULLY"
            if latency_ms and latency_ms > sla_deadline_ms:
                final_status = "COMPLETED_SLA_BREACH"
        elif dispatch_status in ["FAILED", "TIMEOUT", "PENDING"]:
            final_status = "FAILED_EXECUTION"
        
        # --- Create and embed the final log data ---
        transaction_state["final_log_data"] = {
            "final_status": final_status,
            "reconciliation_match": (dispatch_status in ["SUCCESS", "COMPLETED"]),
            "score_drift_detected": False # Placeholder for the learning loop
        }
        
        print(f"✅ CONTROLLER: Final status for {txn_id} is '{final_status}'")

        # --- Persist the entire final state to CockroachDB ---
        print(f"▶️ CONTROLLER: Storing final record for {txn_id} in CockroachDB...")
        await db_client.upsert_transaction_record(transaction_state)

        return transaction_state 