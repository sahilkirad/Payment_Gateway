import cohere
from .config import COHERE_API_KEY

co = cohere.Client(COHERE_API_KEY)

SYSTEM_PROMPT = """
You are an intent classification model for a payment orchestration system.
Classify the user query into exactly one of these intents:
- disbursement_request
- transaction_query
- compliance_check
- fraud_check
- sla_status
- reconciliation
- explainability

Respond only with the intent name and a confidence score between 0 and 1.
"""

def classify_intent(query: str):
    """
    Use Cohere LLM to classify user query into predefined intents.
    """
    response = co.chat(
        model="command-r-plus",
        preamble=SYSTEM_PROMPT,
        message=query,
    )

    # LLM response e.g. "intent: disbursement_request, confidence: 0.92"
    raw = response.text.strip().lower()

    intent, confidence = None, 0.7  # fallback defaults

    if "disbursement" in raw:
        intent = "disbursement_request"
    elif "transaction" in raw:
        intent = "transaction_query"
    elif "compliance" in raw:
        intent = "compliance_check"
    elif "fraud" in raw or "suspicious" in raw:
        intent = "fraud_check"
    elif "sla" in raw:
        intent = "sla_status"
    elif "reconcile" in raw:
        intent = "reconciliation"
    elif "explain" in raw:
        intent = "explainability"

    # naive confidence parse (optional refine with regex)
    if "0." in raw:
        try:
            confidence = float(raw.split("0.")[1][:2]) / 100
        except:
            pass

    return {"intent": intent, "confidence": confidence}
