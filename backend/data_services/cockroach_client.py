import asyncpg
import os
import json
import logging
from typing import Dict, Any

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CockroachDBClient:
    _pool = None

    async def connect(self):
        if self._pool: return
        try:
            conn_str = (
                f"postgresql://{os.getenv('DB_USER', 'sahil')}:"
                f"{os.getenv('DB_PASSWORD', '6So4-7R_fyZu4RFGoUphZw')}@"
                f"{os.getenv('DB_HOST', 'newer-cuscus-14747.j77.aws-us-east-1.cockroachlabs.cloud')}:"
                f"{os.getenv('DB_PORT', '26257')}/"
                f"{os.getenv('DB_NAME', 'arealis_db')}"
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
    
    async def save_agent_result(self, agent_name: str, txn_id: str, result: dict):
        if not self._pool:
            raise ConnectionError("Database pool is not initialized. Call connect() first.")

        query = """
        INSERT INTO agent_results (txn_id, agent, result_json, created_at)
        VALUES ($1, $2, $3, NOW())
        ON CONFLICT (txn_id, agent) DO UPDATE SET
            result_json = EXCLUDED.result_json,
            created_at = NOW();
        """
        try:
            async with self._pool.acquire() as conn:
                await conn.execute(query, txn_id, agent_name, json.dumps(result))
            logger.info(f"✅ Saved agent result for {agent_name} on txn_id {txn_id}")
        except Exception as e:
            logger.error(f"❌ Failed to save agent result for {agent_name}, txn_id={txn_id}: {e}")


db_client = CockroachDBClient() 