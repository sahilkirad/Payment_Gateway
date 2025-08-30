from fastapi import FastAPI
import boto3, json
import psycopg2
import os

app = FastAPI()

@app.get("/test-fetch")
def test_fetch():
    # Connect to S3 (credentials from `aws configure`)
    s3 = boto3.client("s3")
    
    bucket = "sallma"                 # your bucket
    key = "transactions.json"         # file inside that bucket
    
    obj = s3.get_object(Bucket=bucket, Key=key)
    data = json.loads(obj["Body"].read())
    
    # Edge Gateway enrichment
    for txn in data:
        txn["processed"] = True
    
    # Connect to CockroachDB
    conn = psycopg2.connect(os.getenv("CRDB_DSN"))
    cur = conn.cursor()
    
    for txn in data:
        cur.execute(
            "INSERT INTO transactions (txn_id, amount, status, processed) VALUES (%s, %s, %s, %s)",
            (txn["txn_id"], txn["amount"], txn["status"], txn["processed"])
        )
    conn.commit()
    cur.close()
    conn.close()
    
    return {"message": "Fetched from S3, enriched, and stored!", "count": len(data)}
