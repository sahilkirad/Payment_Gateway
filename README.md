# S3 Parquet Reader with Ray

This script reads Parquet data from an AWS S3 bucket using the Ray framework.

## Prerequisites

1. Python 3.8 or higher
2. AWS credentials configured
3. Ray framework installed

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Set up AWS credentials:
```bash
export AWS_ACCESS_KEY_ID="your_access_key"
export AWS_SECRET_ACCESS_KEY="your_secret_key"
```

Or configure AWS CLI:
```bash
aws configure
```

## Usage

Run the script:
```bash
python s3_parquet_reader.py
```

## Features

- ✅ Ray cluster initialization and cleanup
- ✅ AWS credentials validation
- ✅ S3 Parquet data reading
- ✅ Schema display
- ✅ First record preview
- ✅ Comprehensive error handling
- ✅ Logging for debugging

## Configuration

The script is configured to read from `s3://sallma`. To change the S3 path, modify the `s3_path` variable in the `main()` function.
