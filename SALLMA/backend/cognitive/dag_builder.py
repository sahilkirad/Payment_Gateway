# SALLMA/backend/cognitive/dag_builder.py
 
import ray
from typing import Dict, Any
from SALLMA.backend.lams.compliance_agent import ComplianceAgent
from SALLMA.backend.agents.fraud_agent import FraudAgent
from SALLMA.backend.agents.controller_agent import ControllerAgent

def build_dag(intent: str, request: Dict) -> ray.ObjectRef:
    """
    Dynamically builds and executes a workflow using Ray actors.
    Agents are created dynamically when the function is called.
    Returns an ObjectRef that can be passed to ray.get().
    """
    if intent == "high_value_transaction":
        # Create agents dynamically
        fraud_agent = FraudAgent.remote()
        compliance_agent = ComplianceAgent.remote()
        controller_agent = ControllerAgent.remote()
        
        # Parallel execution: FraudAgent and ComplianceAgent
        fraud_result_ref = fraud_agent.execute.remote(request)
        compliance_result_ref = compliance_agent.execute.remote(request)
        
        # Wait for both results in parallel
        fraud_result, compliance_result = ray.get([fraud_result_ref, compliance_result_ref])
        
        # Pass both results as separate arguments to controller
        final_decision_ref = controller_agent.aggregate_results.remote(fraud_result, compliance_result)
        
    elif intent == "low_value_transaction":
        # Create agents dynamically
        fraud_agent = FraudAgent.remote()
        controller_agent = ControllerAgent.remote()
        
        fraud_result_ref = fraud_agent.execute.remote(request)
        fraud_result = ray.get(fraud_result_ref)
        final_decision_ref = controller_agent.aggregate_results.remote(fraud_result)
        
    else:
        # Create controller agent dynamically
        controller_agent = ControllerAgent.remote()
        final_decision_ref = controller_agent.aggregate_results.remote({"default_check": "PASS"})
        
    return final_decision_ref