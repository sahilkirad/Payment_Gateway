# routing_planner/main.py

import json
from agents.routing_planner.config import TxnContext, PlannerState
from agents.routing_planner.route_selector import build_graph

from agents.routing_planner.config import TxnContext, PlannerState
from agents.routing_planner.route_selector import build_graph
import ray
from dotenv import load_dotenv
load_dotenv()

@ray.remote
def run(transaction: dict, feedback: list = None):
    import json
    from agents.routing_planner.config import TxnContext, PlannerState
    from agents.routing_planner.route_selector import build_graph
    # from config import TxnContext, PlannerState
    # from route_selector import build_graph

    """
    Run the routing planner for a given transaction.

    Args:
        transaction (dict): A dictionary containing txn_id, zone, sla_deadline_ms,
                            fee_sensitivity, latency_sensitivity, risk_appetite.
        feedback (list, optional): List of recent feedback dictionaries.
                                   Defaults to an empty list.

    Returns:
        dict: {
            "selection": {...},
            "routes": [...],
            "traces": [...]
        }
    """
    print("▶️ Running Routing Planner Agent...")

    try:
        if feedback is None:
            feedback = []

        # 1. Build TxnContext from transaction dict
        ctx = TxnContext(
            txn_id=transaction.get("txn_id"),
            zone=transaction.get("zone", "UNKNOWN"),
            sla_deadline_ms=transaction.get("sla_deadline_ms", 200),
            fee_sensitivity=transaction.get("fee_sensitivity", 0.5),
            latency_sensitivity=transaction.get("latency_sensitivity", 0.5),
            risk_appetite=transaction.get("risk_appetite", 0.5),
        )

        # 2. Wrap context & feedback in PlannerState
        state = PlannerState(ctx=ctx, recent_feedback=feedback)

        # 3. Build graph and invoke planner
        app = build_graph()
        result_state = app.invoke(state)

        # 4. Return clean structured output
        return {
            "selection": result_state["selection"],
            "routes": [r.model_dump() for r in result_state["routes"]],
            "traces": result_state["traces"],
        }

    except Exception as e:
        import traceback, json
        tb = traceback.format_exc()
        print(f"🔥 Routing Planner failed: {e}\n{tb}")
        return {
            "error": str(e),
            "traceback": tb
        }


# Optional: standalone run for debugging
if __name__ == "__main__":
    txn = {
        "txn_id": "TXN_LG_001",
        "zone": "WEST-1",
        "sla_deadline_ms": 180,
        "fee_sensitivity": 0.6,
        "latency_sensitivity": 0.8,
        "risk_appetite": 0.3,
    }

    feedback = [
        
    ]

    result = run(txn, feedback)

    print("\n=== ROUTING PLAN ===")
    print(json.dumps(result["selection"], indent=2))

    print("\n=== ROUTES DETAIL ===")
    print(json.dumps(result["routes"], indent=2))

    print("\n=== REASONING TRACES ===")
    print(json.dumps(result["traces"], indent=2))