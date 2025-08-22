# SALLMA/backend/main.py

import os
import sys
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import the compiled agent from your ml folder
from ml.agents.routing_graph import app as routing_agent_app

# Load Environment Variables and Set Up Database Connection
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not found in .env file.")

if DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "cockroachdb://", 1)

# This path correctly looks for ca.crt in the project's root directory
ca_cert_path = "ca.crt"
if not os.path.exists(ca_cert_path):
    raise FileNotFoundError("ca.crt not found. Please ensure it is in the root directory of your project (SALLMA/ca.crt).")

connect_args = {"sslmode": "verify-full", "sslrootcert": ca_cert_path}
engine = create_engine(DATABASE_URL, connect_args=connect_args)

# --- API Definition ---
api = FastAPI(
    title="SALLMA Orchestration API",
    description="An API for dynamically generating payment routing DAGs using an LLM agent.",
    version="1.0.0"
)

# --- UPDATED: API Input Structure ---
# The API now only needs the transaction ID. The intent will be fetched from the database.
class DagRequest(BaseModel):
    txn_id: str

# Define the list of agents
ALLOWED_AGENTS_LIST = [
    "edge_gateway", "zone_classifier", "routing_planner", "validator", "aml_agent",
    "kyc_verifier", "sla_guardian", "confidence", "dispatch", "fallback_mutator",
    "ledger_writer", "explainability", "reconciliation", "sla_auditor"
]

# --- Create the API Endpoint ---
@api.post("/generate-dag")
async def generate_dag(request: DagRequest):
    """
    Accepts a transaction ID, fetches the full transaction data (including intent)
    from the database, and invokes the LangGraph agent to generate a routing DAG.
    """
    try:
        # 1. Fetch the transaction data from the database
        print(f"Fetching data for txn_id: {request.txn_id}")
        # We now query the 'client_data' table
        query = f"SELECT * FROM client_data WHERE txn_id = '{request.txn_id}' LIMIT 1"
        df = pd.read_sql(query, engine)
        
        if df.empty:
            raise HTTPException(status_code=404, detail=f"Transaction with ID '{request.txn_id}' not found.")
            
        transaction_details = df.to_dict('records')[0]
        
        # --- UPDATED: Get the intent dynamically from the fetched data ---
        # We safely get the 'intent' from the database record.
        intent = transaction_details.get('intent', 'Unknown Intent')
        print(f"Found intent for {request.txn_id}: '{intent}'")
        
        # 2. Prepare the initial state for the agent
        initial_state = {
            "transaction_details": transaction_details,
            "intent": intent, # This is now the dynamic intent from the database
            "allowed_agents": ALLOWED_AGENTS_LIST
        }
        
        # 3. Invoke the agent asynchronously
        print(f"Invoking agent for txn_id: {request.txn_id}")
        final_state = await routing_agent_app.ainvoke(initial_state)
        
        # 4. Return the agent's decision
        return final_state.get("routing_decision")

    except Exception as e:
        print(f"🔥 An error occurred: {e}")
        raise HTTPException(status_code=500, detail=str(e))