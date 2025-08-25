import ray
from agents.confidence_scorer.main import run

# 1. Start Ray
ray.init()

# 2. Example fake transaction
sample_txn = {
    "txn_id": "12345-abc",
    "amount": 75000,
    "priority": "HIGH",
    "geo_state": "Maharashtra",
    "zone": "MH-West",
    "timestamp": "2025-08-17T12:00:00"
}

# 3. Run Confidence Scorer agent
result = ray.get(run.remote(sample_txn))

print("✅ Confidence Scorer Output:")
print(result)

# 4. Shutdown Ray when done
ray.shutdown()
