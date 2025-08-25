import ray
import importlib
from typing import Dict, Any, List

# Your AGENT_FUNCTION_MAP is correct for your file structure.
AGENT_FUNCTION_MAP = {
    "edge_gateway": "agents.edge_gateway.main.run",
    "validator": "backend.agents.validator.main.run",
    "aml_agent": "backend.agents.aml_agent.main.run",
    "kyc_verifier": "backend.agents.kyc_verifier.main.run",
    "zone_classifier": "agents.zone_classifier.main.run",
    "routing_planner": "agents.routing_planner.main.run",
    "confidence_scorer": "agents.confidence_scorer.main.run",
    "fraud_scorer": "backend.agents.fraud_scorer.main.run",
    "sla_guardian": "backend.agents.sla_guardian.main.run",
    "dispatch": "backend.agents.dispatch.main.run",
    "fallback_mutator": "backend.agents.fallback_mutator.main.run",
    "ledger_writer": "backend.agents.ledger_writer.main.run",
    "explainability": "backend.agents.explainability.main.run",
    "reconciliation": "backend.agents.reconciliation.main.run",
    "sla_auditor": "backend.agents.sla_auditor.main.run"
}

def execute_dag(dag: Dict[str, Any], transaction_details: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parses a DAG and executes its "worker" agents in parallel using Ray.
    """
    print("\n--- Starting Ray DAG Execution ---")
    
    nodes = dag.get("nodes", [])
    edges = dag.get("edges", [])
    
    task_refs: Dict[str, ray.ObjectRef] = {}
    dependencies = {node: [edge[0] for edge in edges if edge[1] == node] for node in nodes}
    launched_nodes = set()

    while len(launched_nodes) < len(nodes):
        ready_to_launch = [
            node for node in nodes 
            if node not in launched_nodes and all(dep in launched_nodes for dep in dependencies[node])
        ]

        if not ready_to_launch:
            if len(launched_nodes) < len(nodes):
                print("🔥 Error: DAG execution stalled. Check for cycles or missing nodes.")
            break

        for agent_name in ready_to_launch:
            # The controller is handled separately after this orchestration.
            if agent_name == "controller" or agent_name not in AGENT_FUNCTION_MAP:
                launched_nodes.add(agent_name)
                continue

            parent_refs = [task_refs[parent] for parent in dependencies[agent_name]]
            # --- CRITICAL CHANGE: We now call _launch_task with the original transaction details for every agent ---
            task_refs[agent_name] = _launch_task(agent_name, transaction_details, parent_refs)
            launched_nodes.add(agent_name)
    
    # Wait for all launched tasks to complete.
    print("\n--- Waiting for all Ray worker tasks to complete... ---")
    final_results = {name: ray.get(ref) for name, ref in task_refs.items()}
    
    print("--- Ray DAG Execution Complete ---")
    return final_results

def _launch_task(agent_name: str, transaction: dict, dep_refs: List[ray.ObjectRef]) -> ray.ObjectRef:
    """Helper function to dynamically import and launch a Ray task."""
    try:
        module_path = AGENT_FUNCTION_MAP[agent_name]
        parts = module_path.split('.')
        module_name, func_name = ".".join(parts[:-1]), parts[-1]
        
        agent_module = __import__(module_name, fromlist=[func_name])
        agent_func = getattr(agent_module, func_name)
        
        print(f"🚀 Launching Ray task: {agent_name}")
        # --- CRITICAL CHANGE ---
        # We now pass the original transaction details to EVERY agent,
        # followed by the results of its parent agents (*dep_refs).
        return agent_func.remote(transaction, *dep_refs)
            
    except Exception as e:
        error_msg = f"Error launching agent '{agent_name}': {e}"
        print(f"🔥 {error_msg}")
        return ray.put({"status": "error", "reason": error_msg})
    
dag = {
  "nodes": [
    "edge_gateway",
    "zone_classifier",
    "confidence_scorer",
    "routing_planner",
    "controller"
  ],
  "edges": [
    [
      "edge_gateway",
      "zone_classifier"
    ],
    [
      "edge_gateway",
      "confidence_scorer"
    ],
    [
      "zone_classifier",
      "routing_planner"
    ],
    [
      "confidence_scorer",
      "routing_planner"
    ]
  ]
}

t = {'txn_id': 'TXN-175E84A084', 'amount': 42289.73, 'account': 'GB73MWSJ79406171080992', 'ifsc': 'HDFC0000001', 'vendor_id': 'VND6672', 'sla_id': 'SLA-MEDIUM-720', 'kyc_hash': 'kyc_06e6e888859c48959c4ebdffbc116660', 'consent_hash': 'consent_d9af258775234b8baacb5fd3a75d2fad', 'timestamp': '2025-08-11T10:34:36.495924Z', 'priority': 'MEDIUM', 'aadhaar_hash': 'aadhaar_38700e0275364954ae43b5b50e510f52', 'gstin': '18AAAAA8697v9Z3', 'routing_hints': 'FAST_ROUTE', 'source_app': 'payment_gateway', 'fraud_score': 0.0827, 'confidence_score': 0.736, 'intent': 'Vendor Payment'}
print(execute_dag(dag, t))