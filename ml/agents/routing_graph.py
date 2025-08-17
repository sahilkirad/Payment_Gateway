# SALLMA/ml/agents/routing_graph.py

import os
from typing import TypedDict
from langchain_cohere import ChatCohere
from langchain_core.prompts import ChatPromptTemplate
from pydantic import BaseModel, Field
# --- NEW: Import the JSON parser ---
from langchain_core.output_parsers import JsonOutputParser
from langgraph.graph import StateGraph, END
from dotenv import load_dotenv

# Load environment variables at the very top to ensure they are available
load_dotenv()

# --- Define State and Output Schema (No changes needed) ---
class RoutingState(TypedDict):
    transaction_details: dict
    intent: str
    allowed_agents: list[str]
    # This will hold the final output
    routing_decision: dict

# We will now ask the LLM to describe the agent workflow in its reasoning
class RoutePrediction(BaseModel):
   nodes: list[str] = Field(description="A list of agent names that form the execution DAG.")
   edges: list[list[str]] = Field(description="A list of lists, where each inner list represents an edge from one agent to another.")
   workflow_justification: str = Field(description="A concise, human-readable explanation for why this specific DAG was constructed, referencing the transaction's risk signals and intent.")
# --- UPDATED: Initialize the Cohere LLM and a JSON Parser ---
# We use the standard 'command' model which is available on the trial tier.
llm = ChatCohere(model="command", temperature=0)
# This parser will tell the LLM how to format its JSON output.
parser = JsonOutputParser(pydantic_object=RoutePrediction)


# --- Define Nodes and Graph ---
def analyze_and_decide(state: RoutingState) -> dict:
    """Analyzes transaction data and produces a structured routing decision using a standard JSON prompt."""
    print("▶️ Node: analyze_and_decide")
    transaction = state['transaction_details']
    intent = state['intent']
    allowed = state['allowed_agents']
    
    # --- UPDATED: A much more detailed prompt that requests JSON output ---
    prompt = ChatPromptTemplate.from_template(
       
        """
You are an orchestration planner for a Payment Routing Gateway. 
Your task is to produce ONLY a valid JSON object that defines the execution DAG for the given transaction.
 
### Allowed agents (use only these names):
{allowed}
 
### Agent purposes (short):
- edge_gateway: entry point; normalizes the API payload into internal txn_master.
- zone_classifier: classifies zone/region/rail preference using account/IFSC/routing_hints.
- routing_planner: selects the optimal payment rail/bank based on scores, fees, hints, and SLA posture.
- validator: basic schema/sanity/consent/format checks; gates all downstream risk/compliance.
- aml_agent: sanctions/PEP/watchlist rules; domain rule-checks (post-validator).
- kyc_verifier: checks KYC/KYB records/consent hashes (post-validator).
- sla_guardian: evaluates SLA contract posture and predicted breach risk; informs routing and controller.
- controller: aggregates all agent outputs into a single execution plan for human approval.
- dispatch: sends the payment to the selected rail/bank API per the controller plan.
- fallback_mutator: constructs the next-best route/rail if dispatch fails or times out.
- ledger_writer: persists the system decision context and execution events to the immutable ledger.
- explainability: materializes causality/trace for auditors and developers from the ledger + decisions.
- reconciliation: matches post-execution results and updates reconciliation status.
- sla_auditor: calculates actual vs expected SLA and records any breaches.
 
### Global rules:
- Always include validator and controller.
- aml_agent and kyc_verifier must run AFTER validator (never before).
- routing_planner must run AFTER zone_classifier and AFTER any SLA posture signals (from sla_guardian if used).
- controller must run AFTER all checks that influence execution (e.g., aml_agent, kyc_verifier, sla_guardian, routing_planner).
- dispatch must run AFTER controller.
- On dispatch failure/timeout, fallback_mutator produces a new plan that goes BACK to dispatch.
- ledger_writer must run AFTER a dispatch attempt (success or after fallback path).
- explainability, reconciliation, and sla_auditor typically run AFTER ledger_writer.
- Do NOT include agents that are irrelevant given the transaction + intent. Prune safely but respect dependencies.
 
### Intent-aware & transaction-aware heuristics (guidance, not hard rules):
- If intent is "salary disbursement" or other low-risk payouts AND priority is LOW, you may keep a lean path but NEVER skip validator. 
- If confidence_score < 0.60 OR fraud_score is provided and looks risky (>= 0.50), always include aml_agent and kyc_verifier before controller.
- If SLA is strict (SLA name contains HIGH/GOLD/FAST), include sla_guardian to inform routing_planner; if SLA is LOW or relaxed, sla_guardian may still be included to record posture.
- routing_planner should not appear before zone_classifier.
- Human-in-the-loop approval happens outside the DAG; ensure controller is present to produce a final plan for that UI step.
 
### Output format (STRICT):
Return ONLY a valid JSON object (no markdown, no commentary, no trailing commas). 
Provide a concise justification explaining WHY you chose this specific workflow. 
Keys must be double-quoted. Edges must reference nodes you include. 
Structure:
{{
  "nodes": ["agent_name_1", "agent_name_2", "..."],
  "edges": [["from_agent", "to_agent"], ["from_agent2", "to_agent3"], "..."]
  
}}
### Output Instructions (STRICT):
        {format_instructions}
### Inputs:
Intent: {intent}
 
Transaction JSON:
{txn}
    """
)
    
    # The chain now pipes the prompt to the LLM, and then the LLM's output to the JSON parser.
    chain = prompt | llm | parser
    
    try:
        # We invoke the chain with the necessary inputs, including the format instructions from the parser.
        response = chain.invoke({
           
            "allowed":allowed,
            "intent": intent,
            "txn": str(transaction),
            "format_instructions": parser.get_format_instructions(),
        })
        print(f"✅ LLM Response")
        return {"routing_decision": response}
    except Exception as e:
        print(f"🔥 LLM Error: {e}")
        return {"routing_decision": {"error": str(e)}}

# --- UPDATED: Compile the graph once and export it ---
# This is more efficient as the server doesn't have to rebuild the graph on every request.
workflow = StateGraph(RoutingState)
workflow.add_node("analyze_and_decide", analyze_and_decide)
workflow.set_entry_point("analyze_and_decide")
workflow.add_edge("analyze_and_decide", END)

# The compiled, runnable agent is now a global variable named 'app'
app = workflow.compile()