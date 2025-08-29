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

### Active Agents (only use these names):
- edge_gateway
- zone_classifier
- confidence_scorer
- routing_planner
- controller

### Agent Roles & Dependencies:
1. edge_gateway
   - Input: API request headers + transaction body
   - Output: normalized_batch (txn_master)
   - ALL other agents depend on its normalized output (must always run first)

2. zone_classifier
   - Input: normalized transaction from edge_gateway
   - Output: {{ "zone": <region> }}
   - Used by routing_planner for geographic/scheme preferences

3. confidence_scorer
   - Input: normalized transaction from edge_gateway
   - Output: confidence_scores for routes
   - Runs in parallel to zone_classifier since both consume the normalized transaction
   - Its scores must be passed into routing_planner

4. routing_planner
   - Input: 
       - normalized transaction (edge_gateway)
       - zone (zone_classifier)
       - confidence_scores (confidence_scorer)
   - Output: selected route, ranking, rationale
   - Must run AFTER both zone_classifier and confidence_scorer complete

5. controller
    - Input: 
        - normalized transaction from edge_gateway
        - final route decision from routing_planner
    - Output: API response to client
    - Must run last

### DAG Rules:
- Always start with edge_gateway
- zone_classifier and confidence_scorer run IN PARALLEL (after edge_gateway)
- routing_planner runs AFTER both zone_classifier and confidence_scorer
- controller runs LAST at all times
- Only include these 5 agents in the DAG (do not add others)
- Return ONLY valid JSON in the format below (STRICT, no extra text, no markdown)

### Output JSON Format:
{{
  "nodes": ["agent1", "agent2", ...],
  "edges": [["from_agent", "to_agent"], ...]
}}

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