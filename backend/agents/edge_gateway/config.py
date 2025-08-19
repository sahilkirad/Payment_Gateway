# backend/agents/edge_gateway/config.py

GATEWAY_CONFIG = {
    "max_requests_per_minute": 1000,
    "batch_size_limit": 500,
    "sla_timeout_minutes": 5,
    "require_api_key": True
}
