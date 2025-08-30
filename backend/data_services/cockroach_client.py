import asyncpg
import os
import json
import logging
from typing import Dict, Any

from dotenv import load_dotenv
load_dotenv()
print("DB_USER:", os.getenv("DB_USER"))
print("DB_PASSWORD:", os.getenv("DB_PASSWORD"))
print("DB_HOST:", os.getenv("DB_HOST"))
print("DB_PORT:", os.getenv("DB_PORT"))
print("DB_NAME:", os.getenv("DB_NAME"))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CockroachDBClient:
    _pool = None

    async def connect(self):
        if self._pool: return
        try:
            conn_str = (
                f"postgresql://{os.getenv('DB_USER')}:"
                f"{os.getenv('DB_PASSWORD')}@"
                f"{os.getenv('DB_HOST')}:"
                f"{os.getenv('DB_PORT')}/"
                f"{os.getenv('DB_NAME')}"
            )
            self._pool = await asyncpg.create_pool(dsn=conn_str, min_size=1, max_size=10)
            logger.info("✅ Successfully connected to CockroachDB and created connection pool.")
        except Exception as e:
            logger.error(f"❌ CRITICAL: Failed to connect to CockroachDB: {e}")
            raise

    async def close(self):
        if self._pool:
            await self._pool.close()
            logger.info("✅ CockroachDB connection pool closed.")

    async def upsert_transaction_record(self, transaction_state: Dict[str, Any]):
        if not self._pool:
            raise ConnectionError("Database pool is not initialized. Call connect() first.")
        
        txn_id = transaction_state.get("txn_id")
        if not txn_id:
            logger.error("❌ Cannot upsert record: txn_id is missing.")
            return

        key_fields = {
            "txn_id": txn_id,
            "status": transaction_state.get("status"),
            "final_status": transaction_state.get("final_log_data", {}).get("final_status"),
            "risk_flag": transaction_state.get("risk_flag"),
            "kyc_valid": transaction_state.get("kyc_valid"),
            "fallback_used": transaction_state.get("final_log_data", {}).get("fallback_used"),
        }
        full_graph_json = json.dumps(transaction_state)

        query = """
        INSERT INTO operational_ledger (
            txn_id, status, final_status, risk_flag, kyc_valid, fallback_used, full_decision_graph, updated_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
        ON CONFLICT (txn_id) DO UPDATE SET
            status = EXCLUDED.status,
            final_status = EXCLUDED.final_status,
            risk_flag = EXCLUDED.risk_flag,
            kyc_valid = EXCLUDED.kyc_valid,
            fallback_used = EXCLUDED.fallback_used,
            full_decision_graph = EXCLUDED.full_decision_graph,
            updated_at = NOW();
        """
        try:
            async with self._pool.acquire() as conn:
                await conn.execute(query, *key_fields.values(), full_graph_json)
            logger.info(f"✅ Successfully upserted record for txn_id: {txn_id}")
        except Exception as e:
            logger.error(f"❌ Failed to upsert record for {txn_id}: {e}")

db_client = CockroachDBClient() 