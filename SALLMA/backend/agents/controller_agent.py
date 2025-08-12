import ray
import logging
from typing import Any, Dict

@ray.remote
class ControllerAgent:
    def __init__(self):
        logging.info("ControllerAgent Actor Initialized.")

    def aggregate_results(self, *results: Dict[str, Any]) -> Dict:
        logging.info(f"--- ControllerAgent: Received results: {results} ---")
        print(f"--- ControllerAgent: Received results: {results} ---")
        
        final_details = {}
        for res_dict in results:
            final_details.update(res_dict)

        decision = "APPROVE"
        if "FLAG" in str(final_details.values()):
            decision = "REJECT"

        final_decision_obj = {"final_decision": decision, "details": final_details}
        
        logging.info(f"--- ControllerAgent: Returning final decision: {final_decision_obj} ---")
        print(f"--- ControllerAgent: Returning final decision: {final_decision_obj} ---")
        return final_decision_obj