import ray
from typing import Dict, Any
 
# Example Ray remote definitions for Agents (assuming these are implemented in sallma.agents)
# Placeholders here for demonstration; actual implementation should be in sallma/agents/*
@ray.remote
class FraudAgent:
    def execute(self, request: Dict) -> Dict:
        # Fraud detection logic here
        # For demo returns mock result
        return {"fraud_check": "pass"}
 
@ray.remote
class ComplianceAgent:
    def execute(self, request: Dict, fraud_check_result: Dict) -> Dict:
        # Compliance checks with fraud results considered
        return {"compliance_check": "pass"}
 
@ray.remote
class SLAAgent:
    def execute(self, request: Dict, compliance_result: Dict) -> Dict:
        # SLA validation logic
        return {"sla_check": "pass"}
 
@ray.remote
class ControllerAgent:
    def aggregate_results(self, results: Dict[str, Any]) -> Dict:
        # Final aggregation and decision
        # For demo, simple logic:
        if all(v == "pass" for v in results.values()):
            decision = "Approve"
        else:
            decision = "Reject"
        return {"final_decision": decision, "details": results}
 
 
def build_dag(intent: str, request: Dict) -> ray.ObjectRef:
    fraud_agent = FraudAgent.remote()
    compliance_agent = ComplianceAgent.remote()
    sla_agent = SLAAgent.remote()
    controller_agent = ControllerAgent.remote()

    if intent == "high_value_transaction":
        fraud_result_ref = fraud_agent.execute.remote(request)
        fraud_result = ray.get(fraud_result_ref)

        compliance_result_ref = compliance_agent.execute.remote(request, fraud_result)
        compliance_result = ray.get(compliance_result_ref)

        sla_result_ref = sla_agent.execute.remote(request, compliance_result)
        sla_result = ray.get(sla_result_ref)

        results = {
            "fraud_check": fraud_result["fraud_check"],
            "compliance_check": compliance_result["compliance_check"],
            "sla_check": sla_result["sla_check"]
        }

        decision_ref = controller_agent.aggregate_results.remote(results)
        return decision_ref

    elif intent == "low_value_transaction":
        fraud_result_ref = fraud_agent.execute.remote(request)
        fraud_result = ray.get(fraud_result_ref)

        results = {
            "fraud_check": fraud_result["fraud_check"]
        }

        decision_ref = controller_agent.aggregate_results.remote(results)
        return decision_ref

    elif intent == "account_query":
        compliance_result_ref = compliance_agent.execute.remote(request, {})
        compliance_result = ray.get(compliance_result_ref)

        results = {
            "compliance_check": compliance_result["compliance_check"]
        }

        decision_ref = controller_agent.aggregate_results.remote(results)
        return decision_ref

    else:  # general_request
        decision_ref = controller_agent.aggregate_results.remote({})
        return decision_ref
