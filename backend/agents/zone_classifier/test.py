# test_zone_classifier.py
from backend.agents.zone_classifier.bank_affinity import get_zone

def test_zone_lookup():
    assert get_zone("HDFC000123") == "NATIONAL"
    assert get_zone("UTIB001234") == "SOUTH"
    assert get_zone("XXXX000000") == "UNKNOWN"
    print("✅ Zone lookup passed")

test_zone_lookup()
