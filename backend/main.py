# # SALLMA/backend/main.py
# import ray
# import sys
# import os
# from fastapi import FastAPI, HTTPException
# from pydantic import BaseModel
# from sqlalchemy import create_engine
# import pandas as pd
# from dotenv import load_dotenv

# # --- Initialize Ray when the server starts ---
# if not ray.is_initialized():
#     ray.init()

# # Add project root to path to allow imports from other folders
# sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# # Import the Planner and the Orchestrator
# from ml.agents.routing_graph import app as routing_agent_app
# # from backend.orchestrator import execute_dag

# # --- Database setup ---
# load_dotenv()
# DATABASE_URL = os.getenv("DATABASE_URL")
# if not DATABASE_URL:
#     raise RuntimeError("DATABASE_URL not found.")
# if DATABASE_URL.startswith("postgresql://"):
#     DATABASE_URL = DATABASE_URL.replace("postgresql://", "cockroachdb://", 1)
# ca_cert_path = "C:/Users/PRATHAMESH/.postgresql/root.crt"
# if not os.path.exists(ca_cert_path):
#     raise FileNotFoundError("ca.crt not found in the project root directory.")
# connect_args = {"sslmode": "verify-full", "sslrootcert": ca_cert_path}
# engine = create_engine(DATABASE_URL, connect_args=connect_args)

# # --- API Definition ---
# api = FastAPI(title="SALLMA Orchestration API", version="1.0.0")

# class TransactionRequest(BaseModel):
#     txn_id: str

# # This list is passed to the LLM Planner
# ALLOWED_AGENTS_LIST = [
#     "edge_gateway", "zone_classifier", "routing_planner", "validator", "aml_agent",
#     "kyc_verifier", "sla_guardian", "confidence_scorer", "fraud_scorer", "dispatch",
#     "fallback_mutator", "ledger_writer", "explainability", "reconciliation", "sla_auditor"
# ]

# @api.post("/process-transaction")
# async def process_transaction(request: TransactionRequest):
#     """
#     Full end-to-end processing:
#     1. Generates a dynamic DAG using the LLM Planner.
#     2. Executes the DAG in parallel using the Ray Orchestrator.
#     3. Triggers the Controller agent after execution is complete.
#     """
#     try:
#         # 1. Fetch transaction data
#         print(f"--- Step 1: Fetching data for txn_id: {request.txn_id} ---")
#         query = f"SELECT * FROM client_data WHERE txn_id = '{request.txn_id}' LIMIT 1"
#         df = pd.read_sql(query, engine)
#         if df.empty:
#             raise HTTPException(status_code=404, detail=f"Transaction '{request.txn_id}' not found.")
#         transaction_details = df.to_dict('records')[0]
#         intent = transaction_details.get('intent', 'Unknown Intent')
#         print(f"✅ Found intent: '{intent}'")
        
#         # 2. Invoke the Planner Agent to generate the DAG
#         print(f"\n--- Step 2: Invoking Planner Agent to generate DAG ---")
#         initial_state = {
#             "transaction_details": transaction_details,
#             "intent": intent,
#             "allowed_agents": ALLOWED_AGENTS_LIST
#         }
#         final_state = await routing_agent_app.ainvoke(initial_state)
#         decision_dag = final_state.get("routing_decision")
#         if not decision_dag or "error" in decision_dag:
#              raise HTTPException(status_code=500, detail=f"Failed to generate DAG: {decision_dag.get('error')}")
#         print("✅ DAG generated successfully.")

#         # 3. --- Execute the generated DAG using the Ray Orchestrator ---
#         # This will automatically call the agents in the correct, parallel order.
#         # Each agent will save its own results to the database.
#         # execution_results = execute_dag(decision_dag, transaction_details)

#         # 4. --- Trigger the Controller Agent (Future Step) ---
#         # Now that all worker agents have run and saved their data, you can call the Controller.
#         # controller_result = await controller_agent.generate_final_plan.remote(request.txn_id)
#         # print(f"\n✅ Orchestration for {request.txn_id} complete. Ready for Controller.")

#         # 5. Return the final results
#         return {
#             "generated_dag": decision_dag
#         }

#     except Exception as e:
#         print(f"🔥 An error occurred: {e}")
#         raise HTTPException(status_code=500, detail=str(e))

# @api.on_event("shutdown")
# def shutdown_event():
#     ray.shutdown()

# SALLMA/backend/main.py

import ray
import sys
import os
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv
import json
import asyncio

from agents.routing_planner.main import run as routing_planner_run
from agents.confidence_scorer.main import run as confidence_scorer_run
from agents.zone_classifier.main import run as zone_classifier_run
from agents.edge_gateway.main import run as edge_gateway_run

# --- Initialize Ray ---
if not ray.is_initialized():
    ray.init()

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import the Planner Agent (DAG generator)
from ml.agents.routing_graph import app as routing_agent_app

# --- Database setup ---
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not found.")

if DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "cockroachdb://", 1)

ca_cert_path = "C:/Users/PRATHAMESH/.postgresql/root.crt"
if not os.path.exists(ca_cert_path):
    raise FileNotFoundError("ca.crt not found in the project root directory.")

connect_args = {
    "sslmode": "verify-full",
    "sslrootcert": ca_cert_path
}
engine = create_engine(DATABASE_URL, connect_args=connect_args)

# --- FastAPI ---
api = FastAPI(title="SALLMA Orchestration API", version="1.0.0")


class TransactionRequest(BaseModel):
    txn_id: str


@ray.remote
def controller(transaction, routing_output):
    print("🎛️ Running Controller...")
    return {
        "txn_id": transaction["txn_id"],
        "approved": True,
        "final_plan": routing_output
    }


# ======================
# === DAG EXECUTOR ===
# ======================
async def execute_dag(decision_dag, transaction):
    """Executes agents as per the generated DAG using Ray."""
    node_outputs = {}

    # 1. Map agent names to Ray functions
    agent_map = {
        "edge_gateway": edge_gateway_run,
        "zone_classifier": zone_classifier_run,
        "confidence_scorer": confidence_scorer_run,
        "routing_planner": routing_planner_run,
        "controller": controller
    }

    # 2. Prepare execution ordering
    nodes = decision_dag["nodes"]
    edges = decision_dag["edges"]
    dependencies = {n: [] for n in nodes}

    for src, dst in edges:
        dependencies[dst].append(src)

    executed = set()
    results = {}

    while len(executed) < len(nodes):
        ready_nodes = [
            n for n in nodes
            if n not in executed and all(dep in executed for dep in dependencies[n])
        ]

        tasks = []

        for node in ready_nodes:
            agent_fn = agent_map[node]

            # Prepare args based on dependencies
            if node == "edge_gateway":
                tasks.append((node, agent_fn.remote(transaction)))

            elif node == "zone_classifier":
                tasks.append((node, agent_fn.remote(results["edge_gateway"])))

            elif node == "confidence_scorer":
                tasks.append((node, agent_fn.remote(results["edge_gateway"])))

            elif node == "routing_planner":
                tasks.append((
                    node,
                    agent_fn.remote(transaction, [results["zone_classifier"], results["confidence_scorer"]])
                ))

            elif node == "controller":
                tasks.append((node, agent_fn.remote(transaction, results["routing_planner"])))

            else:
                print(f"⚠️ Unknown agent {node}, skipping.")

        if tasks:
            resolved = ray.get([t[1] for t in tasks])

            for (node, _), output in zip(tasks, resolved):
                results[node] = output
                executed.add(node)
                print(f"✅ {node} completed → {output}")

    return results


# ======================
# === FASTAPI ENDPOINT ===
# ======================
@api.post("/process-transaction")
async def process_transaction(request: TransactionRequest):
    try:
        # 1. Fetch transaction from DB
        query = f"SELECT * FROM client_data WHERE txn_id = '{request.txn_id}' LIMIT 1"
        df = pd.read_sql(query, engine)

        if df.empty:
            raise HTTPException(status_code=404, detail=f"Transaction '{request.txn_id}' not found.")

        transaction_details = df.to_dict('records')[0]
        intent = transaction_details.get('intent', 'Unknown Intent')
        print(f"✅ Fetched transaction with intent '{intent}'")

        # 2. Generate DAG via Planner Agent
        initial_state = {
            "transaction_details": transaction_details,
            "intent": intent,
            "allowed_agents": [
                "edge_gateway",
                "zone_classifier",
                "confidence_scorer",
                "routing_planner",
                "controller"
            ]
        }

        final_state = await routing_agent_app.ainvoke(initial_state)
        decision_dag = final_state.get("routing_decision")

        if not decision_dag:
            raise HTTPException(status_code=500, detail="Planner failed to generate DAG")

        print(f"✅ DAG Generated: {json.dumps(decision_dag, indent=2)}")

        # 3. Execute DAG with Ray
        results = await execute_dag(decision_dag, transaction_details)

        return {
            "generated_dag": decision_dag,
            "execution_results": results
        }

    except Exception as e:
        print(f"🔥 Error in orchestration: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@api.on_event("shutdown")
def shutdown_event():
    ray.shutdown()
