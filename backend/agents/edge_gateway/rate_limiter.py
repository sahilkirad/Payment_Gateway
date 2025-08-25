# backend/agents/edge_gateway/rate_limiter.py
# import redis

# r = redis.Redis(host="localhost", port=6379, db=0)

def check_rate_limit(max_requests_per_minute: int, merchant_id: str = "default") -> tuple[bool, str]:
    # key = f"rate:{merchant_id}"
    # current = r.get(key)
    # if current and int(current) >= max_requests_per_minute:
    #     return False, "Rate limit exceeded"
    # r.incr(key)
    # r.expire(key, 60)  # 1-minute window
    return True, "OK"
