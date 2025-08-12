# s3_parquet_reader.py

import ray
import sys
import logging
import os
from typing import Optional
from dotenv import load_dotenv

# Import our pipeline functions
from sallma_pipelines.pipelines.graph_transformer import transform_to_graph
from sallma_pipelines.pipelines.neo4j_loader import Neo4jLoader

# Load environment variables (.env file)
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def check_credentials() -> bool:
    """Checks for both AWS and Neo4j credentials."""
    aws_ok = os.getenv('AWS_ACCESS_KEY_ID') and os.getenv('AWS_SECRET_ACCESS_KEY')
    neo4j_ok = os.getenv('NEO4J_URI') and os.getenv('NEO4J_USERNAME') and os.getenv('NEO4J_PASSWORD')
    
    if not aws_ok: logging.error("AWS credentials not found in .env file.")
    if not neo4j_ok: logging.error("Neo4j credentials not found in .env file.")
    
    return aws_ok and neo4j_ok

def read_s3_parquet_data(s3_path: str) -> Optional[ray.data.Dataset]:
    """Reads Parquet data from an S3 path."""
    try:
        logging.info(f"Attempting to read from: {s3_path}")
        return ray.data.read_parquet(s3_path)
    except Exception as e:
        logging.error(f"Failed to read from S3: {e}")
        return None

def main():
    """Main function to orchestrate the ETL pipeline."""
    logging.info("Starting ETL process: Read -> Transform -> Load.")

    if not check_credentials():
        return 1

    s3_path = "s3://sallma"

    try:
        ray.init(ignore_reinit_error=True)
        logging.info("Ray cluster initialized.")

        # 1. READ data from S3
        dataset = read_s3_parquet_data(s3_path)
        if not dataset:
            return 1

        # 2. TRANSFORM data into graph format
        logging.info("Applying graph transformation...")
        graph_dataset = dataset.map_batches(
            transform_to_graph,
            batch_format="pyarrow" # Use efficient Arrow format
        )

        # 3. LOAD data into Neo4j
        logging.info("Loading transformed data into Neo4j...")
        # Use map_batches with the Neo4jLoader class
        load_stats = graph_dataset.map_batches(
            Neo4jLoader,
            batch_size=100, # Smaller batches for DB writes
            batch_format="pandas",
            num_cpus=1, # Use fewer CPUs for DB writes to not overwhelm it
            concurrency=1
        )
        
        # Trigger the full pipeline execution and get final stats
        total_loaded = {"nodes": 0, "relationships": 0}
        for stats in load_stats.iter_rows():
            total_loaded["nodes"] += stats.get("nodes_created", 0)
            total_loaded["relationships"] += stats.get("relationships_created", 0)

        logging.info("=== ETL PIPELINE COMPLETE ===")
        logging.info(f"Total nodes loaded: {total_loaded['nodes']}")
        logging.info(f"Total relationships loaded: {total_loaded['relationships']}")
        return 0

    except Exception as e:
        logging.error(f"A critical error occurred: {e}")
        return 1
    finally:
        if ray.is_initialized():
            ray.shutdown()
            logging.info("Ray cluster shut down.")

if __name__ == "__main__":
    sys.exit(main())