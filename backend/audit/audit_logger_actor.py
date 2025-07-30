from dotenv import load_dotenv
import os
import boto3
import json
import ray

load_dotenv()

# Initialize Ray (this should ideally only be called once in your main.py)
# ray.init()

@ray.remote
def upload_json_to_s3(data, bucket_name, s3_key):
    """
    Upload JSON serializable data to an S3 bucket at the specified key.

    Args:
        data (dict): JSON-serializable Python dict to upload.
        bucket_name (str): Name of the target S3 bucket.
        s3_key (str): S3 object key (path + filename).
    """
    try:
        s3 = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_DEFAULT_REGION')
        )

        json_data = json.dumps(data)

        # Upload to S3
        s3.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=json_data,
            ContentType='application/json'
        )

        print(f"Uploaded JSON to s3://{bucket_name}/{s3_key}")
        return f"Successfully uploaded {s3_key}"

    except Exception as e:
        print(f"Error uploading to S3: {e}")
        return f"Error uploading {s3_key}: {str(e)}"
