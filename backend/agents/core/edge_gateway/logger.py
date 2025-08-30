# edge_gateway/logger.py
import json, time

def log(event: str, **kwargs):
    payload = {"ts": int(time.time()*1000), "event": event, **kwargs}
    print(json.dumps(payload, default=str))
