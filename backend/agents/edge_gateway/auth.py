# backend/agents/edge_gateway/auth.py
from fastapi import HTTPException

# Dummy auth; replace with DB or secret store lookup
def validate_auth(headers: dict) -> tuple[bool, str]:
    api_key = headers.get("x-api-key")
    merchant_id = headers.get("x-merchant-id")
    if not api_key or not merchant_id:
        return False, "Missing API key or merchant ID"
    if api_key != "supersecret":
        return False, "Invalid API key"
    return True, "OK"
