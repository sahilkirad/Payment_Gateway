import boto3
import json
import os
from dotenv import load_dotenv
from statistics import mean
from collections import Counter
import cohere
import ray

# Load environment variables from .env file
load_dotenv()

@ray.remote
class ExplanationAgent:
    def __init__(self):
        # Initialize AWS credentials
        self.AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
        self.AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
        self.AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')

        # Initialize Cohere credentials
        self.COHERE_API_KEY = os.getenv('COHERE_API_KEY')

        # Setup clients
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=self.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=self.AWS_SECRET_ACCESS_KEY,
            region_name=self.AWS_REGION
        )

        self.cohere_client = cohere.ClientV2(self.COHERE_API_KEY)

    def fetch_s3_json_data(self, bucket_name, folder_path):
        try:
            response = self.s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_path)

            if 'Contents' not in response:
                print(f"No objects found in {bucket_name}/{folder_path}")
                return []

            json_data = []
            for obj in response['Contents']:
                object_key = obj['Key']
                if object_key.endswith('.json'):
                    s3_object = self.s3_client.get_object(Bucket=bucket_name, Key=object_key)
                    json_content = json.loads(s3_object['Body'].read().decode('utf-8'))
                    json_data.append(json_content)

            return json_data

        except Exception as e:
            print(f"Error fetching data from S3: {str(e)}")
            return []

    def generate_metrics(self, transactions):
        fraud_flags = [t['fraud_flag'] for t in transactions]
        risk_scores = [t['risk_score'] for t in transactions]
        actions = [t['action'] for t in transactions]
        reasons = [t['reasoning'] for t in transactions]

        metrics = {
            "total_transactions": len(transactions),
            "fraudulent_transactions": sum(fraud_flags),
            "non_fraudulent_transactions": len(fraud_flags) - sum(fraud_flags),
            "average_risk_score": round(mean(risk_scores), 2) if risk_scores else 0,
            "most_common_action": Counter(actions).most_common(1)[0][0] if actions else None,
            "most_common_reasons": [r for r, _ in Counter(reasons).most_common(3)]
        }

        return metrics

    def generate_explanation(self, metrics):
        try:
            metrics_text = (
                f"Total transactions: {metrics['total_transactions']}\n"
                f"Fraudulent transactions: {metrics['fraudulent_transactions']}\n"
                f"Non-fraudulent transactions: {metrics['non_fraudulent_transactions']}\n"
                f"Average risk score: {metrics['average_risk_score']}\n"
                f"Most common action: {metrics['most_common_action']}\n"
                f"Top 3 reasons: {', '.join(metrics['most_common_reasons'])}"
            )

            messages = [
                {
                    "role": "user",
                    "content": (
                        "You are a state-of-the-art expert in transaction analytics. "
                        "Here are the transaction metrics:\n\n"
                        f"{metrics_text}\n\n"
                        "Please generate a concise, professional, and human-readable explanation of these metrics for a business audience."
                    )
                }
            ]

            response = self.cohere_client.chat(
                model="command-a-03-2025",
                messages=messages,
            )

            return response

        except Exception as e:
            print(f"Error generating explanation with Cohere: {str(e)}")
            return "Unable to generate explanation due to an error."

    def run(self, bucket_name, folder_path):
        data = self.fetch_s3_json_data(bucket_name, folder_path)

        if data:
            metrics = self.generate_metrics(data)
            print("Metrics:")
            print(json.dumps(metrics, indent=2))

            explanation = self.generate_explanation(metrics)
            print("\nExplanation:")
            print(explanation)
        else:
            print("No data to process.")
