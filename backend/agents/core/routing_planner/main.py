# routing_planner/main.py

import json
from .config import TxnContext, PlannerState
from .route_selector import build_graph


if __name__ == "__main__":
    ctx = TxnContext(
        txn_id="TXN_LG_001",
        zone="WEST-1",
        sla_deadline_ms=180,
        fee_sensitivity=0.6,
        latency_sensitivity=0.8,
        risk_appetite=0.3,
    )

    feedback = [
        {"txn_id": "T-1", "final_status": "SUCCESS", "sla_breached": False, "cost_over_budget": False},
        {"txn_id": "T-2", "final_status": "FAILED", "sla_breached": False, "cost_over_budget": False},
        {"txn_id": "T-3", "final_status": "SUCCESS", "sla_breached": True, "cost_over_budget": False},
    ]

    state = PlannerState(ctx=ctx, recent_feedback=feedback)
    app = build_graph()
    result_state = app.invoke(state)

    print("\n=== ROUTING PLAN ===")
    print(json.dumps(result_state["selection"], indent=2))

    print("\n=== ROUTES DETAIL ===")
    print(json.dumps([r.model_dump() for r in result_state["routes"]], indent=2))

    print("\n=== REASONING TRACES ===")
    print(json.dumps(result_state["traces"], indent=2))
