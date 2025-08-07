import ray
import logging
from typing import Dict

@ray.remote
class FraudAgent:
    def __init__(self):
        logging.info("FraudAgent Actor Initialized.")

    def execute(self, request: Dict) -> Dict:
        logging.info("--- FraudAgent: Processing request ---")
        print("--- FraudAgent: Processing request ---")
        amount = request.get("amount", 0)
        
        if amount > 10000:
            result = {"fraud_check": "FLAG", "fraud_score": 0.9}
        else:
            result = {"fraud_check": "PASS", "fraud_score": 0.1}
        
        logging.info(f"--- FraudAgent: Returning result: {result} ---")
        print(f"--- FraudAgent: Returning result: {result} ---")
        return result