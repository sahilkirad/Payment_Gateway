# Ingest data from S3 using Ray

import ray
import sys
import logging
import os
from typing import Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def check_aws_credentials() -> bool:
    """
    Check if AWS credentials are available.
    
    Returns:
        bool: True if credentials are found, False otherwise
    """
    # Check for AWS credentials in environment variables
    aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    
    # Check for AWS profile
    aws_profile = os.getenv('AWS_PROFILE')
    
    # Check for AWS credentials file (common location)
    aws_credentials_file = os.path.expanduser('~/.aws/credentials')
    aws_config_file = os.path.expanduser('~/.aws/config')
    
    if aws_access_key and aws_secret_key:
        logging.info("AWS credentials found in environment variables")
        return True
    elif aws_profile:
        logging.info(f"AWS profile '{aws_profile}' found in environment")
        return True
    elif os.path.exists(aws_credentials_file) or os.path.exists(aws_config_file):
        logging.info("AWS credentials/config files found")
        return True
    else:
        logging.warning("No AWS credentials found!")
        logging.warning("Please ensure one of the following:")
        logging.warning("1. Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables")
        logging.warning("2. Set AWS_PROFILE environment variable")
        logging.warning("3. Configure AWS credentials using 'aws configure'")
        logging.warning("4. Use IAM roles (if running on AWS infrastructure)")
        return False

def read_s3_parquet_data(s3_path: str) -> Optional[ray.data.Dataset]:
    """
    Read Parquet data from S3 using Ray.
    
    Args:
        s3_path (str): S3 path to the Parquet files
        
    Returns:
        Optional[ray.data.Dataset]: Ray dataset or None if failed
    """
    dataset = None
    
    try:
        # Initialize Ray cluster
        logging.info("Initializing Ray cluster...")
        ray.init()
        logging.info("Ray cluster initialized successfully")
        
        # Read Parquet data from S3
        logging.info(f"Reading Parquet data from: {s3_path}")
        dataset = ray.data.read_parquet(s3_path)
        logging.info("Data read successfully")
        
        # Print schema information
        logging.info("Dataset schema:")
        print(f"Schema: {dataset.schema()}")
        
        # Print first record
        logging.info("First record:")
        first_record = dataset.take(1)
        if first_record:
            print(f"First record: {first_record[0]}")
        else:
            logging.warning("Dataset appears to be empty")
            
        return dataset
        
    except FileNotFoundError as e:
        logging.error(f"S3 path not found: {s3_path}. Please check if the bucket and path exist.")
        logging.error(f"Details: {str(e)}")
        return None
        
    except PermissionError as e:
        logging.error(f"Permission denied accessing S3 path: {s3_path}")
        logging.error("Please check your AWS credentials and bucket permissions.")
        logging.error(f"Details: {str(e)}")
        return None
        
    except ConnectionError as e:
        logging.error(f"Network connection error while accessing S3: {s3_path}")
        logging.error("Please check your internet connection and AWS region settings.")
        logging.error(f"Details: {str(e)}")
        return None
        
    except ValueError as e:
        logging.error(f"Invalid S3 path format: {s3_path}")
        logging.error("Please ensure the path follows the format: s3://bucket-name/path")
        logging.error(f"Details: {str(e)}")
        return None
        
    except Exception as e:
        logging.error(f"Unexpected error occurred: {str(e)}")
        logging.error("This might be due to:")
        logging.error("1. Missing or incorrect AWS credentials")
        logging.error("2. Network connectivity issues")
        logging.error("3. Incorrect S3 bucket name or path")
        logging.error("4. Insufficient permissions to access the bucket")
        logging.error("5. Invalid Parquet file format")
        return None
        
    finally:
        # Always shutdown Ray cluster
        if ray.is_initialized():
            logging.info("Shutting down Ray cluster...")
            ray.shutdown()
            logging.info("Ray cluster shut down successfully")

def main():
    """Main function to execute the S3 Parquet reading process."""
    # Define S3 path
    s3_path = "s3://sallma"
    
    logging.info("Starting S3 Parquet data ingestion process")
    
    # Check AWS credentials first
    if not check_aws_credentials():
        logging.error("AWS credentials check failed. Cannot proceed with S3 access.")
        return 1
    
    # Read the data
    dataset = read_s3_parquet_data(s3_path)
    
    if dataset is not None:
        logging.info("Data ingestion completed successfully")
        return 0
    else:
        logging.error("Data ingestion failed")
        return 1

if __name__ == "__main__":
    sys.exit(main())