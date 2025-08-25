# backend/agents/zone_classifier/bank_affinity.py
from agents.zone_classifier.zone_mapping import IFSC_TO_ZONE, DEFAULT_ZONE

def get_zone(ifsc: str) -> str:
    """Resolve IFSC prefix to a zone/affinity group."""
    if not ifsc or len(ifsc) < 4:
        return DEFAULT_ZONE
    prefix = ifsc[:4].upper()
    return IFSC_TO_ZONE.get(prefix, DEFAULT_ZONE)
