# confidence_scorer/features.py

def extract_features(transaction: dict) -> dict:
    """
    Extract relevant features for scoring a transaction's routing confidence.
    """
    return {
        "txn_id": transaction.get("txn_id"),
        "amount": transaction.get("amount"),
        "priority": transaction.get("priority"),
        "geo_state": transaction.get("geo_state"),
        "zone": transaction.get("zone", "UNKNOWN"),
        "timestamp": transaction.get("timestamp"),
    }
