# main.py
import ray
from ray import serve
from fastapi import FastAPI
import logging
import time
import sys
import os
 
# --- START OF PERMANENT FIX ---
# This block forces Python to look for the 'SALLMA' package in the correct parent directory.
# It resolves the ModuleNotFoundError and ensures the correct modules are always loaded.
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
# --- END OF PERMANENT FIX ---
 
# Now, the imports will work correctly
from SALLMA.backend.cognitive.intent_manager import detect_intent
from SALLMA.backend.cognitive.dag_builder import build_dag
#from SALLMA.audit.audit_logger_actor import AuditLoggerActor
#from SALLMA.data.schemas import AgentOutput # Assuming you have this for type hints
 
# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
app = FastAPI(title="SALLMA Processing API")
 
@serve.deployment
@serve.ingress(app)
class SALLMA_App:
    def __init__(self):
        # No persistent agents - they will be created dynamically in the workflow
        print("SALLMA_App initialized - agents will be created dynamically")

    @app.post("/process_transaction")
    async def process_transaction(self, request: dict) -> dict:
        """
        This is the main endpoint that receives a request and orchestrates the agent workflow.
        """
        try:
            logging.info(f"Received new request: {request}")
            print("Request received:", request)

            intent = detect_intent(request)
            logging.info(f"Detected intent: {intent}")

            # Build and execute workflow - agents will be created when executed
            agent_dag = build_dag(intent, request)
            logging.info("Successfully built execution workflow.")

            # Execute the workflow and get the result
            final_decision = ray.get(agent_dag)
            logging.info(f"Final decision from Controller: {final_decision}")

            return final_decision
            
        except Exception as e:
            logging.error(f"Error processing transaction: {str(e)}")
            return {"error": str(e), "status": "failed"}
 
if __name__ == "__main__":
    ray.init(namespace="sallma", ignore_reinit_error=True)
    serve.start(http_options={"host": "0.0.0.0", "port": 8000})
   
    sallma_app = SALLMA_App.bind()
    serve.run(sallma_app)
   
    print("\nApplication is running at http://0.0.0.0:8000/docs")
    print("Press Ctrl+C to shut down.")
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        print("\nShutting down the server...")
        serve.shutdown()
        