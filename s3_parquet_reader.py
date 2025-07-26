import ray
import sys
import logging
import os
import time
from typing import Optional
from dotenv import load_dotenv
from sallma.pipelines.graph_transformer import process_dataset_to_graph, get_complete_graph_statistics

# Import the graph transformation function
from sallma.pipelines.graph_transformer import transform_to_graph

# Load environment variables from .env file
load_dotenv()

# Configure logging once
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def check_aws_credentials() -> bool:
    """Checks if AWS credentials are available from standard sources."""
    if os.getenv('AWS_ACCESS_KEY_ID') and os.getenv('AWS_SECRET_ACCESS_KEY'):
        logging.info("AWS credentials found in environment variables.")
        return True
    if os.getenv('AWS_PROFILE'):
        logging.info(f"AWS profile '{os.getenv('AWS_PROFILE')}' found in environment.")
        return True
    aws_credentials_file = os.path.expanduser('~/.aws/credentials')
    aws_config_file = os.path.expanduser('~/.aws/config')
    if os.path.exists(aws_credentials_file) or os.path.exists(aws_config_file):
        logging.info("AWS credentials/config file found.")
        return True

    logging.warning("No AWS credentials found!")
    return False

def read_s3_parquet_data(s3_path: str) -> Optional[ray.data.Dataset]:
    """Reads Parquet data from an S3 path and handles potential errors."""
    try:
        logging.info(f"Attempting to read Parquet data from: {s3_path}")
        dataset = ray.data.read_parquet(s3_path)
        logging.info("Data read successfully from S3.")
        return dataset
    except Exception as e:
        logging.error(f"An unexpected error occurred while reading from S3: {e}")
        return None

def main():
    """Main function to orchestrate the data ingestion process."""
    logging.info("Starting S3 Parquet data ingestion process.")

    if not check_aws_credentials():
        logging.error("AWS credentials check failed. Aborting process.")
        return 1

    s3_path = "s3://sallma"

    try:
        ray.init()
        logging.info("Ray cluster initialized successfully.")

        dataset = read_s3_parquet_data(s3_path)

        if dataset:
            logging.info(f"Successfully processed dataset with {dataset.count()} records.")
            

            # Transform the entire dataset to graph format and get complete statistics
            logging.info("Starting complete graph transformation of entire dataset...")
            graph_statistics = get_complete_graph_statistics(dataset)
            
            # Log comprehensive statistics
            logging.info("=== GRAPH TRANSFORMATION COMPLETE ===")
            logging.info(f"Total unique nodes: {graph_statistics['total_unique_nodes']}")
            logging.info(f"Total unique relationships: {graph_statistics['total_unique_relationships']}")
            
            logging.info("Node distribution by type:")
            for node_type, count in graph_statistics['node_distribution'].items():
                logging.info(f"  - {node_type}: {count}")
            
            logging.info("Relationship distribution by type:")
            for rel_type, count in graph_statistics['relationship_distribution'].items():
                logging.info(f"  - {rel_type}: {count}")
            
            logging.info("=== END GRAPH STATISTICS ===")

            # Apply graph transformation to the dataset
            logging.info("Starting graph transformation...")
            transformed_dataset = dataset.map_batches(transform_to_graph)
            
            # Show some results from the transformation
            logging.info("Transformation completed. Showing sample results...")
            sample_results = transformed_dataset.take(1)
            if sample_results:
                sample = sample_results[0]
                logging.info(f"Sample transformation result: {sample.get('batch_stats', {})}")

            
            # --- PAUSE SCRIPT HERE TO VIEW DASHBOARD ---
            logging.info("Dashboard should now be available at http://127.0.0.1:8265")
            logging.info("Pausing for 60 seconds... Press Ctrl+C in the terminal to exit early.")
            time.sleep(60) # This line will pause the script for 60 seconds
            # ---------------------------------------------
            
            logging.info("Pause finished. Continuing...")
            return 0
        else:
            logging.error("Data ingestion failed.")
            return 1

    except Exception as e:
        logging.error(f"A critical error occurred in the main process: {e}")
        return 1
    finally:
        if ray.is_initialized():
            logging.info("Shutting down Ray cluster...")
            ray.shutdown()
            logging.info("Ray cluster shut down successfully.")

if __name__ == "__main__":
    os.environ['PYTHONUTF8'] = '1'
    sys.exit(main())