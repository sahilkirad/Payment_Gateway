# main.py

import ray
from ray import serve
from fastapi import FastAPI
import logging

# Import the components from your teammates
from sallma.cognitive.intent_manager import detect_intent
from sallma.cognitive.dag_builder import build_dag
from sallma.audit.audit_logger_actor import AuditLoggerActor
from sallma.data.schemas import AgentOutput # Assuming you have this for type hints

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Create a FastAPI app instance
# Ray Serve will use this to handle HTTP requests
app = FastAPI()

@serve.deployment
@serve.ingress(app)
class SALLMA_App:
    def __init__(self):
        # This is where you would initialize any models or resources
        # The AuditLoggerActor is started separately, so we just get a handle to it.
        self.audit_logger = ray.get_actor("AuditLogger", namespace="sallma")

    @app.post("/process_transaction")
    async def process_transaction(self, request: dict) -> dict:
        """
        This is the main endpoint that receives a request and orchestrates the agent workflow.
        """
        logging.info(f"Received new request: {request}")

        # 1. Call Arnavi's Cognitive Layer to get a plan
        intent = detect_intent(request)
        logging.info(f"Detected intent: {intent}")
        
        agent_dag = build_dag(intent, request)
        logging.info("Successfully built execution DAG.")

        # 2. Execute the DAG
        # The result will be the final output from Deepanshu's Controller Agent
        final_decision = await agent_dag.execute_async()
        logging.info(f"Final decision from Controller: {final_decision}")

        # The Controller is responsible for calling the AuditLogger,
        # so we don't need to do it here. We just return the result.
        return final_decision

# --- Application Startup Logic ---
if __name__ == "__main__":
    # Initialize Ray
    ray.init(namespace="sallma", ignore_reinit_error=True)

    # Start the persistent AuditLoggerActor
    # This actor will live for the duration of the Ray cluster.
    audit_logger = AuditLoggerActor.options(name="AuditLogger", lifetime="detached").remote()
    
    # Bind and deploy the Ray Serve application
    sallma_app = SALLMA_App.bind()
    serve.run(sallma_app, host="0.0.0.0", port=8000)