# confidence_scorer/scorer.py

from openai import OpenAI
from agents.confidence_scorer.config import LLM_MODEL, TEMPERATURE, MAX_TOKENS
from dotenv import load_dotenv
import os

load_dotenv()

# client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def llm_confidence_scorer(features: dict) -> dict:
    # """
    # Uses an LLM to reason about the transaction and return confidence scores
    # for available banks.
    # """
    # prompt = f"""
    # You are a risk assessment agent for a payment gateway.
    # Based on the following transaction details, assign a confidence score (0.0 to 1.0)
    # for 3 banks: Bank_A, Bank_B, Bank_C.
    # Higher confidence = more reliable to process this transaction.

    # Transaction Details:
    # - ID: {features['txn_id']}
    # - Amount: {features['amount']}
    # - Priority: {features['priority']}
    # - Zone: {features['zone']}
    # - Geo State: {features['geo_state']}
    # - Timestamp: {features['timestamp']}

    # Respond in valid JSON like:
    # {{
    #     "Bank_A": 0.85,
    #     "Bank_B": 0.65,
    #     "Bank_C": 0.40
    # }}
    # """

    # response = client.chat.completions.create(
    #     model=LLM_MODEL,
    #     messages=[{"role": "user", "content": prompt}],
    #     temperature=TEMPERATURE,
    #     max_tokens=MAX_TOKENS
    # )

    # # Parse response as JSON
    # try:
    #     scores = eval(response.choices[0].message.content.strip())
    # except Exception:
    scores = {"Bank_A": 0.5, "Bank_B": 0.5, "Bank_C": 0.5}  # fallback

    return scores
