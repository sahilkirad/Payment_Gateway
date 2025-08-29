# backend/agents/edge_gateway/enrich.py
from datetime import datetime

def enrich_payouts(payouts: list[dict]) -> list[dict]:
    enriched = []
    for txn in payouts:
        txn_copy = txn.copy()
        txn_copy['received_at'] = datetime.utcnow().isoformat()
        # TODO: SLA lookup, zone hint, priority, etc.
        enriched.append(txn_copy)
    return enriched
