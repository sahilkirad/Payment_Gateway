# SALLMA/ml/notebooks/generate_training_data.py

import os
import json
import random # Import the random library
from sqlalchemy import create_engine, text, inspect, Table, Column, String, Float, Boolean, MetaData
from dotenv import load_dotenv

# ==============================================================================
# CONFIGURATION
# ==============================================================================
DATA_FILE_PATH = os.path.join('../data/', 'strict_test_dataset_100.json')

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL not found in .env file.")

if DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "cockroachdb://", 1)

# --- NEW: Define a list of possible intents ---
INTENTS = ["Salary Disbursement", "Vendor Payment", "Customer Refund", "Tax Payment", "Loan Disbursal"]


# ==============================================================================
# DATABASE SETUP FUNCTION (With 'intent' column)
# ==============================================================================
def setup_database():
    """Initializes the DB engine and creates/clears the training data table."""
    print("--- Connecting to CockroachDB cluster... ---")
    engine = create_engine(DATABASE_URL)
    metadata = MetaData()

    # --- UPDATED SCHEMA: Added a new column for 'intent' ---
    training_table = Table('model_training_data', metadata,
        Column('txn_id', String, primary_key=True),
        Column('amount', Float),
        Column('account', String),
        Column('ifsc', String),
        Column('vendor_id', String),
        Column('sla_id', String),
        Column('kyc_hash', String),
        Column('consent_hash', String),
        Column('timestamp', String),
        Column('priority', String),
        Column('aadhaar_hash', String),
        Column('gstin', String),
        Column('routing_hints', String),
        Column('source_app', String),
        Column('fraud_score', Float),
        Column('confidence_score', Float),
        Column('intent', String) # The new column
    )

    with engine.connect() as connection:
        print("✅ Database connection successful.")
        inspector = inspect(engine)
        table_name = "model_training_data"
        
        if inspector.has_table(table_name):
            print(f"--- Old table '{table_name}' found. Dropping it to apply new schema... ---")
            connection.execute(text(f'DROP TABLE IF EXISTS {table_name} CASCADE'))
            connection.commit()
            print("✅ Old table dropped.")

        print(f"--- Creating table '{table_name}' with the new schema (including intent)... ---")
        metadata.create_all(engine)
        print(f"✅ Table '{table_name}' created successfully.")
            
    return engine, training_table

# ==============================================================================
# MAIN EXECUTION
# ==============================================================================
if __name__ == "__main__":
    try:
        print(f"--- Loading dataset from {DATA_FILE_PATH} ---")
        if not os.path.exists(DATA_FILE_PATH):
            raise FileNotFoundError(f"The specified data file was not found at {DATA_FILE_PATH}")
        
        with open(DATA_FILE_PATH, 'r') as f:
            dataset = json.load(f)

        # --- NEW: Add a random intent to each record in the dataset ---
        for record in dataset:
            record['intent'] = random.choice(INTENTS)
        
        print(f"✅ Loaded and added dynamic intents to {len(dataset)} records.")

        engine, training_table = setup_database()

        table_columns = [c.name for c in training_table.columns]
        filtered_dataset = [
            {key: record[key] for key in table_columns if key in record}
            for record in dataset
        ]

        print("\n--- Inserting new records (with intent) into CockroachDB... ---")
        with engine.connect() as connection:
            with connection.begin() as transaction:
                connection.execute(training_table.insert(), filtered_dataset)
        
        print(f"\n✅ All {len(filtered_dataset)} records successfully saved to table: 'model_training_data'")

    except Exception as e:
        print(f"🔥 An error occurred: {e}")