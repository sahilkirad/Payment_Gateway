import ray
from SALLMA.backend.cognitive.intent_manager import detect_intent
from SALLMA.backend.cognitive.dag_builder import build_dag
 
 
def main():
    ray.init(include_dashboard=False)

 
    # Sample request example
    request = {
        "user_id": "user123",
        "amount": 15000,
        "action": "initiate_transfer",
        "transaction_id": "txn_001"
    }
 
    intent = detect_intent(request)
    print(f"Detected intent: {intent}")
 
    dag = build_dag(intent, request)
    result = ray.get(dag)
    print(f"Final Decision: {result['final_decision']}")
    print(f"Details: {result['details']}")
 
    ray.shutdown()
 
 
if __name__ == "__main__":
    main()