from .s3_client import fetch_transactions 

txns = fetch_transactions()
print("Total transactions:", len(txns))
print("First transaction:", txns[0])
