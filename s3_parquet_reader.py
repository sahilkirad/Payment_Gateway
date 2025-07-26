# Ingest data from S3 using Ray
import ray
import os
from typing import Optional
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def validate_aws_credentials() -> bool:
    """Validate that AWS credentials are available."""
    aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    
    if not aws_access_key or not aws_secret_key:
        logger.error("AWS credentials not found. Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables.")
        return False
    
    logger.info("AWS credentials validated successfully")
    return True

def main():
    """Main function to read Parquet data from S3 using Ray."""
    try:
        # Initialize Ray cluster
        logger.info("Initializing Ray cluster...")
        if not ray.is_initialized():
            ray.init()
        logger.info("Ray cluster initialized successfully")
        
        # Validate AWS credentials
        if not validate_aws_credentials():
            return
        
        # Define S3 path
        s3_path = "s3://sallma"
        logger.info(f"Reading Parquet data from: {s3_path}")
        
        try:
            # Read Parquet data from S3
            dataset = ray.data.read_parquet(s3_path)
            logger.info(f"Successfully read dataset with {dataset.count()} records")
            
            # Display schema
            logger.info("Dataset schema:")
            print(dataset.schema())
            
            # Display first record
            logger.info("First record:")
            first_record = dataset.take(1)
            if first_record:
                print(first_record[0])
            else:
                logger.warning("No records found in the dataset")
                
        except Exception as e:
            logger.error(f"Error reading data from S3: {e}")
            return
        
    except Exception as e:
        logger.error(f"Error initializing Ray: {e}")
        return
    finally:
        # Shutdown Ray cluster
        if ray.is_initialized():
            logger.info("Shutting down Ray cluster...")
            ray.shutdown()
            logger.info("Ray cluster shut down successfully")

if __name__ == "__main__":
    main()