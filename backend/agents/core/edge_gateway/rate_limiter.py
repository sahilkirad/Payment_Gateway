# edge_gateway/rate_limit.py
import time
from fastapi import HTTPException, status
from .config import settings

# naive per-process bucket {key: [reset_ts, count]}
_BUCKET = {}

def rate_limit(key: str):
    now = int(time.time())
    win = settings.RATE_LIMIT_WINDOW_SEC
    limit = settings.RATE_LIMIT_MAX_REQ

    reset, count = _BUCKET.get(key, (now + win, 0))
    if now > reset:
        reset, count = (now + win, 0)

    count += 1
    _BUCKET[key] = (reset, count)

    if count > limit:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"Rate limit exceeded. Try after {reset - now}s"
        )
