# backend/agents/zone_classifier/zone_mapping.py

# Static mapping for now, can later replace with DB fetch
IFSC_TO_ZONE = {
    "HDFC": "NATIONAL",
    "ICIC": "NATIONAL",
    "SBIN": "NATIONAL",
    "PUNB": "NORTH",
    "UTIB": "SOUTH",
    "KKBK": "WEST",
    "YESB": "EAST"
}

DEFAULT_ZONE = "UNKNOWN"
