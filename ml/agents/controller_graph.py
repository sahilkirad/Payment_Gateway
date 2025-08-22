# SALLMA/backend/agents/controller_graph.py

from typing import TypedDict, Dict, Any
from langchain_cohere import ChatCohere
from langchain_core.prompts import ChatPromptTemplate
from pydantic import BaseModel, Field
from langchain_core.output_parsers import JsonOutputParser
from langgraph.graph import StateGraph, END
from dotenv import load_dotenv

load_dotenv()

# --- 1. Define the State ---
# This is the memory, holding all the data the controller needs to make a decision.
class ControllerState(TypedDict):
    agent_inputs: Dict[str, Any] # A dictionary containing outputs from all previous agents
    execution_plan: Dict[str, Any] # The final, structured plan

# --- 2. Define the Output Schema ---
# This is the final execution plan we want the LLM to generate.
class ExecutionPlan(BaseModel):
    final_routing_key: str = Field(description="The definitive, chosen routing_key for the transaction.")
    final_status: str = Field(description="The final adjudicated status. Must be one of: 'PENDING_APPROVAL', 'FLAGGED_FOR_REVIEW', 'REJECTED'.")
    justification: str = Field(description="A clear, human-readable justification for the final_status, referencing specific agent inputs.")

# --- 3. Initialize the LLM and Parser ---
llm = ChatCohere(model="command", temperature=0)
parser = JsonOutputParser(pydantic_object=ExecutionPlan)

# --- 4. Define the Node ---
def generate_plan_node(state: ControllerState) -> dict:
    """Takes all agent inputs and synthesizes them into a final execution plan."""
    print("▶️ Node: generate_plan_node (Controller)")
    
    agent_inputs = state['agent_inputs']
    
    prompt = ChatPromptTemplate.from_template(
        """You are the Controller Agent, the central decision-maker in a payment gateway.
        Your task is to analyze the outputs from various upstream agents and produce a single, final, and justified execution plan.

        ### Upstream Agent Data:
        {agent_inputs}

        ### Adjudication Rules:
        1.  **REJECTED:** If the 'validator_output' indicates failure OR 'aml_passed' is false OR 'kyc_valid' is false, the transaction MUST be 'REJECTED'.
        2.  **FLAGGED_FOR_REVIEW:** If 'risk_flag' is true OR 'fraud_score' is high (> 0.75) OR 'sla_breach_predicted' is true, the status MUST be 'FLAGGED_FOR_REVIEW'.
        3.  **PENDING_APPROVAL:** For all other cases, the status is 'PENDING_APPROVAL'.
        4.  **Final Routing Key:** The `final_routing_key` should be the `routing_key` provided by the `routing_planner_output`.

        ### Your Task:
        Based on the provided data and the adjudication rules, generate the final execution plan. Provide a clear justification for your decision, citing the specific data points that led to your conclusion.

        ### Output Instructions (STRICT):
        {format_instructions}
        """
    )
    
    chain = prompt | llm | parser
    
    try:
        response = chain.invoke({
            "agent_inputs": str(agent_inputs),
            "format_instructions": parser.get_format_instructions(),
        })
        print("✅ Controller LLM Response Received")
        return {"execution_plan": response}
    except Exception as e:
        print(f"🔥 Controller LLM Error: {e}")
        return {"execution_plan": {"error": str(e)}}

# --- 5. Build and Compile the Graph ---
def get_controller_graph():
    """Builds and returns the compiled Controller agent."""
    workflow = StateGraph(ControllerState)
    workflow.add_node("generate_plan", generate_plan_node)
    workflow.set_entry_point("generate_plan")
    workflow.add_edge("generate_plan", END)
    return workflow.compile()