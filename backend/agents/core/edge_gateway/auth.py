# edge_gateway/auth.py
from fastapi import Header, HTTPException, status
from .config import settings

def require_api_key(x_api_key: str = Header(None)):
    if not x_api_key:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing API-Key header")
    allowed = [k.strip() for k in settings.API_KEYS.split(",") if k.strip()]
    if x_api_key not in allowed:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key")
    return x_api_key
