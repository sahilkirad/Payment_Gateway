import asyncpg
from typing import List, Dict, Any, Optional
from datetime import datetime
from .config import settings


# --- SCHEMA DDL ---
DDL = """
CREATE TABLE IF NOT EXISTS edge_audit (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  request_id UUID NOT NULL,
  merchant_id STRING,
  source STRING,
  items_count INT8,
  received_at TIMESTAMPTZ DEFAULT now(),
  status STRING,
  error STRING
);

CREATE TABLE IF NOT EXISTS transactions_raw (
  txn_id STRING PRIMARY KEY,
  merchant_id STRING,
  vendor_id STRING,
  account STRING,
  ifsc STRING,
  amount DECIMAL,
  currency STRING,
  ts TIMESTAMPTZ,
  priority STRING,
  geo_state STRING,
  kyc_hash STRING,
  sla_target_sec INT8,
  created_at TIMESTAMPTZ DEFAULT now(),
  status STRING DEFAULT 'INGESTED'
);
"""

# --- QUERIES ---
INSERT_AUDIT = """
INSERT INTO edge_audit (request_id, merchant_id, source, items_count, status, error)
VALUES ($1, $2, $3, $4, $5, $6)
"""

UPSERT_TXN = """
UPSERT INTO transactions_raw
(txn_id, merchant_id, vendor_id, account, ifsc, amount, currency, ts, priority, geo_state, kyc_hash, sla_target_sec, status)
VALUES
($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,'INGESTED')
"""


class CockroachClient:
    def __init__(self, dsn: Optional[str] = None):
        self._dsn = dsn or settings.CRDB_DSN
        self._pool: Optional[asyncpg.Pool] = None

    async def connect(self):
        """Create connection pool & ensure schema exists."""
        if self._pool is None:
            try:
                self._pool = await asyncpg.create_pool(
                    dsn=self._dsn,
                    min_size=1,
                    max_size=10,
                    command_timeout=60
                )
                async with self._pool.acquire() as conn:
                    await conn.execute(DDL)
                print("✅ CockroachDB connected & schema ensured.")
            except Exception as e:
                print(f"❌ DB connection failed: {e}")
                raise

    async def close(self):
        """Close pool gracefully."""
        if self._pool:
            await self._pool.close()
            self._pool = None
            print("🔌 CockroachDB connection closed.")

    async def write_audit(
        self,
        request_id: str,
        merchant_id: str,
        source: str,
        count: int,
        status: str,
        error: Optional[str] = None
    ):
        """Insert an audit log entry."""
        if not self._pool:
            raise RuntimeError("DB pool not initialized. Call connect() first.")
        async with self._pool.acquire() as conn:
            await conn.execute(
                INSERT_AUDIT,
                request_id,
                merchant_id,
                source,
                count,
                status,
                error,
            )

    async def upsert_transactions(self, rows: List[Dict[str, Any]]):
        """Bulk upsert transaction rows."""
        if not self._pool:
            raise RuntimeError("DB pool not initialized. Call connect() first.")

        # Convert timestamp strings → datetime
        for r in rows:
            if isinstance(r.get("timestamp"), str):
                r["timestamp"] = datetime.fromisoformat(r["timestamp"].replace("Z", "+00:00"))

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                for r in rows:
                    await conn.execute(
                        UPSERT_TXN,
                        r["txn_id"],
                        r["merchant_id"],
                        r["vendor_id"],
                        r["account"],
                        r["ifsc"],
                        r["amount"],
                        r["currency"],
                        r["timestamp"],
                        r.get("priority", "NORMAL"),
                        r.get("geo_state"),
                        r.get("kyc_hash"),
                        r.get("sla_target_sec", 60),
                    )
