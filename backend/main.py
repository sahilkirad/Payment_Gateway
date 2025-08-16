import os
import sys
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv

# --- Add the project root to the Python path ---
# This allows us to import from the 'ml' and other folders.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# --- Import the compiled agent from your ml folder ---
from ml.agents.routing_graph import app as routing_agent_app

# --- Load Environment Variables and Set Up Database Connection ---
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not found in .env file.")

if DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "cockroachdb://", 1)

# Find the ca.crt file relative to this script's location
script_dir = os.path.dirname(os.path.abspath(__file__))
ca_cert_path = os.path.join(script_dir, "..", "ml", "agents", "ca.crt")
if not os.path.exists(ca_cert_path):
    raise FileNotFoundError(f"ca.crt not found at expected path: {ca_cert_path}")

connect_args = {"sslmode": "verify-full", "sslrootcert": ca_cert_path}
engine = create_engine(DATABASE_URL, connect_args=connect_args)

# --- Define the API Application ---
api = FastAPI(
    title="SALLMA Orchestration API",
    description="An API for dynamically generating payment routing DAGs using an LLM agent.",
    version="1.0.0"
)

# --- Define the API Input Structure ---
# This tells FastAPI what the request body should look like.
class DagRequest(BaseModel):
    txn_id: str
    intent: str = "Generic Payment" # An optional intent with a default value

# --- Define the list of agents ---
ALLOWED_AGENTS_LIST = [
    "edge_gateway", "zone_classifier", "routing_planner", "validator", "aml_agent",
    "kyc_verifier", "sla_guardian", "controller", "dispatch", "fallback_mutator",
    "ledger_writer", "explainability", "reconciliation", "sla_auditor"
]

# --- Create the API Endpoint ---
@api.post("/generate-dag")
async def generate_dag(request: DagRequest):
    """
    Accepts a transaction ID and intent, fetches the transaction data,
    and invokes the LangGraph agent to generate a routing DAG.
    """
    try:
        # 1. Fetch the transaction data from the database
        print(f"Fetching data for txn_id: {request.txn_id}")
        query = f"SELECT * FROM client_data WHERE txn_id = '{request.txn_id}' LIMIT 1"
        df = pd.read_sql(query, engine)
        
        if df.empty:
            raise HTTPException(status_code=404, detail=f"Transaction with ID '{request.txn_id}' not found.")
            
        transaction_details = df.to_dict('records')[0]
        
        # 2. Prepare the initial state for the agent
        initial_state = {
            "transaction_details": transaction_details,
            "intent": request.intent,
            "allowed_agents": ALLOWED_AGENTS_LIST
        }
        
        # 3. Invoke the agent asynchronously
        print(f"Invoking agent for txn_id: {request.txn_id}")
        # Use 'ainvoke' for asynchronous execution, which is best practice for servers
        final_state = await routing_agent_app.ainvoke(initial_state)
        
        # 4. Return the agent's decision
        return final_state.get("routing_decision")

    except Exception as e:
        print(f"🔥 An error occurred: {e}")
        raise HTTPException(status_code=500, detail=str(e))