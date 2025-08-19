# backend/agents/edge_gateway/utils.py

def validate_currency(currency: str, allowed=None):
    if allowed is None:
        allowed = ["INR", "USD"]
    if currency not in allowed:
        raise ValueError(f"Unsupported currency: {currency}")
