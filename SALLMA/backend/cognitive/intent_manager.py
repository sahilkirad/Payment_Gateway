from typing import Dict
 
 
def detect_intent(request: Dict) -> str:
    """
    Detect the intent of the incoming request based on simple rules or metadata.
    Placeholder for more advanced NLP/ML-based intent recognition.
 
    Args:
        request (Dict): Incoming request payload.
 
    Returns:
        str: The detected intent.
    """
    amount = request.get("amount", 0)
    action = request.get("action", "").lower()
 
    if amount > 10000:
        return "high_value_transaction"
    elif action == "query_account":
        return "account_query"
    elif action == "initiate_transfer":
        return "low_value_transaction"
    else:
        return "general_request"