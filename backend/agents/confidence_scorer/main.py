# confidence_scorer/main.py

import ray
from agents.confidence_scorer.features import extract_features
from agents.confidence_scorer.scorer import llm_confidence_scorer

@ray.remote
def run(transaction: dict) -> dict:
    """
    Ray remote function for Confidence Scorer Agent.
    """
    print("▶️ Running Confidence Scorer Agent...")

    # Step 1: Extract features
    features = extract_features(transaction)

    # Step 2: LLM scoring
    scores = llm_confidence_scorer(features)

    # Step 3: Return structured output
    return {
        "status": "success",
        "txn_id": transaction.get("txn_id"),
        "confidence_scores": scores
    }
