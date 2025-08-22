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
 
### Agent Details & Implementation:
1. edge_gateway - API entry point, normalizes payload into internal txn_master (FastAPI - synchronous)
2. zone_classifier - Classifies zone/region/rail preference using account/IFSC/routing_hints (LangGraph for dynamic mapping)
3. confidence_scorer - Continuous scoring based on routing stats and SLA (LangGraph for parallel scoring tools)
4. routing_planner - Selects optimal payment rail/bank based on scores, fees, hints, SLA (LangGraph for multi-factor optimization)
5. validator - Basic schema/sanity/consent/format checks; gates downstream risk/compliance (FastAPI - deterministic)
6. fraud_scorer - ML inference for fraud probability (FastAPI + ML model - latency-sensitive)
7. aml_agent - Anti-Money Laundering checks against sanction lists (CrewAI for rule + knowledge base reasoning)
8. kyc_verifier - Validates KYC/KYB records/consent hashes (FastAPI - deterministic API integration)
9. sla_guardian - Evaluates SLA contract posture and predicts breach risk (LangGraph for predictive rerouting logic)
10. controller - Aggregates all agent outputs into single execution plan for human approval (LangGraph for decision aggregation)
11. dispatch - Sends payment to selected rail/bank API (FastAPI - synchronous, idempotent)
12. fallback_mutator - Constructs next-best route/rail if dispatch fails (LangGraph for dynamic alternative routing)
13. ledger_writer - Persists system decision context to immutable ledger (Kafka → CockroachDB/S3 - no LLM)
14. explainability - Generates causality/trace for auditors from ledger + decisions (LangGraph for natural language tracing)
15. reconciliation - Matches post-execution results and updates status (CrewAI + LangGraph hybrid for continuous learning)
16. sla_auditor - Calculates actual vs expected SLA and records breaches (CrewAI for post-analysis with penalty rules)
 
### Data Sources:
All agents fetch relevant data from CockroachDB tables including:
- Transaction details and metadata
- Agent outputs and intermediate results  
- Intent definitions and business rules
- SLA contracts and performance data
- Historical routing decisions and outcomes
- Fraud patterns and risk models
 
### Global rules:
- Always include validator and controller in every DAG
- aml_agent and kyc_verifier must run AFTER validator (never before)
- routing_planner must run AFTER zone_classifier and AFTER any SLA posture signals
- controller must run AFTER all checks that influence execution (aml_agent, kyc_verifier, sla_guardian, routing_planner, fraud_scorer)
- dispatch must run AFTER controller
- On dispatch failure/timeout, fallback_mutator produces a new plan that goes BACK to dispatch
- ledger_writer must run AFTER a dispatch attempt (success or after fallback path)
- explainability, reconciliation, and sla_auditor typically run AFTER ledger_writer
- Do NOT include agents that are irrelevant given the transaction context
 
### Intent-aware & transaction-aware heuristics:
- For "salary disbursement" or low-risk payouts with LOW priority: lean path but NEVER skip validator
- If confidence_score < 0.60 OR fraud_score >= 0.50: include aml_agent and kyc_verifier before controller
- If SLA contains HIGH/GOLD/FAST: include sla_guardian to inform routing_planner
- If SLA is LOW/relaxed: sla_guardian may still be included for posture recording
- routing_planner should not appear before zone_classifier
- Human approval happens outside DAG; controller produces final plan for UI
 
### Risk-based agent inclusion:
- High amount (>50,000): include fraud_scorer, aml_agent, kyc_verifier
- New vendor/account: include enhanced validation and compliance checks
- International transactions: include additional compliance layers
- High priority: include sla_guardian for proactive SLA management
 
### Output format (STRICT):
Return ONLY a valid JSON object (no markdown, no commentary, no trailing commas). 
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