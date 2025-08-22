# SALLMA/backend/agents/controller_agent.py

import ray
import logging
from typing import Dict, Any
from backend.data_services.cockroach_client import CockroachDBClient
# Import the new graph builder
from backend.agents.controller_graph import get_controller_graph

logger = logging.getLogger(__name__)

@ray.remote
class ControllerAgent:
    def __init__(self):
        self.db_client = CockroachDBClient()
        # Initialize the LangGraph agent when the actor starts
        self.controller_app = get_controller_graph()
        logger.info("✅ LLM-Powered ControllerAgent initialized.")

    async def generate_final_plan(self, txn_id: str, agent_inputs: Dict[str, Any]) -> Dict[str, Any]:
        """
        Invokes the LangGraph agent to create a final plan and persists it.

        Args:
            txn_id: The unique ID of the transaction.
            agent_inputs: A dictionary containing the outputs from all previous agents.
                          e.g., {"validator_output": {...}, "aml_output": {...}}
        """
        await self.db_client.connect()
        logger.info(f"▶️ CONTROLLER: Invoking LLM agent for transaction {txn_id}")

        # --- 1. Invoke the LangGraph Agent ---
        initial_state = {"agent_inputs": agent_inputs}
        final_state = self.controller_app.invoke(initial_state)
        execution_plan = final_state.get("execution_plan")

        if not execution_plan or "error" in execution_plan:
            logger.error(f"🔥 CONTROLLER: Failed to generate execution plan for {txn_id}. Error: {execution_plan.get('error')}")
            return {"error": "Failed to generate execution plan."}

        # --- 2. Persist the Execution Plan to the logs_data table ---
        logger.info(f"   - Persisting final plan for {txn_id} to logs_data...")
        await self.db_client.update_log_record(
            txn_id=txn_id,
            decision_graph=agent_inputs.get("decision_graph", {}), # The DAG from the routing agent
            final_status=execution_plan.get("final_status"),
            # You can add more fields from the plan to be saved
        )
        
        logger.info(f"✅ CONTROLLER: Final plan for {txn_id} created and stored.")
        
        return execution_plan