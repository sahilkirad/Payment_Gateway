# SALLMA/backend/main.py
import ray
import sys
import os
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv

# --- Initialize Ray when the server starts ---
if not ray.is_initialized():
    ray.init()

# Add project root to path to allow imports from other folders
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import the Planner and the Orchestrator
from ml.agents.routing_graph import app as routing_agent_app
from backend.orchestrator import execute_dag

# --- Database setup ---
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not found.")
if DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "cockroachdb://", 1)
ca_cert_path = "ca.crt"
if not os.path.exists(ca_cert_path):
    raise FileNotFoundError("ca.crt not found in the project root directory.")
connect_args = {"sslmode": "verify-full", "sslrootcert": ca_cert_path}
engine = create_engine(DATABASE_URL, connect_args=connect_args)

# --- API Definition ---
api = FastAPI(title="SALLMA Orchestration API", version="1.0.0")

class TransactionRequest(BaseModel):
    txn_id: str

# This list is passed to the LLM Planner
ALLOWED_AGENTS_LIST = [
    "edge_gateway", "zone_classifier", "routing_planner", "validator", "aml_agent",
    "kyc_verifier", "sla_guardian", "confidence_scorer", "fraud_scorer", "dispatch",
    "fallback_mutator", "ledger_writer", "explainability", "reconciliation", "sla_auditor"
]

@api.post("/process-transaction")
async def process_transaction(request: TransactionRequest):
    """
    Full end-to-end processing:
    1. Generates a dynamic DAG using the LLM Planner.
    2. Executes the DAG in parallel using the Ray Orchestrator.
    3. Triggers the Controller agent after execution is complete.
    """
    try:
        # 1. Fetch transaction data
        print(f"--- Step 1: Fetching data for txn_id: {request.txn_id} ---")
        query = f"SELECT * FROM client_data WHERE txn_id = '{request.txn_id}' LIMIT 1"
        df = pd.read_sql(query, engine)
        if df.empty:
            raise HTTPException(status_code=404, detail=f"Transaction '{request.txn_id}' not found.")
        transaction_details = df.to_dict('records')[0]
        intent = transaction_details.get('intent', 'Unknown Intent')
        print(f"✅ Found intent: '{intent}'")
        
        # 2. Invoke the Planner Agent to generate the DAG
        print(f"\n--- Step 2: Invoking Planner Agent to generate DAG ---")
        initial_state = {
            "transaction_details": transaction_details,
            "intent": intent,
            "allowed_agents": ALLOWED_AGENTS_LIST
        }
        final_state = await routing_agent_app.ainvoke(initial_state)
        decision_dag = final_state.get("routing_decision")
        if not decision_dag or "error" in decision_dag:
             raise HTTPException(status_code=500, detail=f"Failed to generate DAG: {decision_dag.get('error')}")
        print("✅ DAG generated successfully.")

        # 3. --- Execute the generated DAG using the Ray Orchestrator ---
        # This will automatically call the agents in the correct, parallel order.
        # Each agent will save its own results to the database.
        execution_results = execute_dag(decision_dag, transaction_details)

        # 4. --- Trigger the Controller Agent (Future Step) ---
        # Now that all worker agents have run and saved their data, you can call the Controller.
        # controller_result = await controller_agent.generate_final_plan.remote(request.txn_id)
        print(f"\n✅ Orchestration for {request.txn_id} complete. Ready for Controller.")

        # 5. Return the final results
        return {
            "generated_dag": decision_dag,
            "execution_results": execution_results
        }

    except Exception as e:
        print(f"🔥 An error occurred: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@api.on_event("shutdown")
def shutdown_event():
    ray.shutdown()