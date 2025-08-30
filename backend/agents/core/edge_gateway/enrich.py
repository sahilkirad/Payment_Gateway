# edge_gateway/enrich.py
from typing import Dict, Any
from datetime import timezone

def normalize_record(rec: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(rec)
    # uppercase IFSC, currency; ensure tz-aware timestamp
    out["ifsc"] = out["ifsc"].upper()
    out["currency"] = out.get("currency", "INR").upper()

    ts = rec["timestamp"]
    if ts.tzinfo is None:
        out["timestamp"] = ts.replace(tzinfo=timezone.utc)

    # simple masking for logs (not for DB)
    out["_mask_account"] = f"****{rec['account'][-4:]}" if rec.get("account") else None
    return out

import os
print(os.getenv("DATABASE_URL"))
