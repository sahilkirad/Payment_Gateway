import pandas as pd
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Set pandas to display more columns
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

def view_table_contents():
    """Connects to the database and prints the first 10 rows of the table."""
    
    # --- Load Database URL from .env file ---
    load_dotenv()
    DATABASE_URL = os.getenv("DATABASE_URL")

    if not DATABASE_URL:
        raise ValueError("DATABASE_URL not found in .env file.")

    # Use the official cockroachdb dialect
    if DATABASE_URL.startswith("postgresql://"):
        DATABASE_URL = DATABASE_URL.replace("postgresql://", "cockroachdb://", 1)

    try:
        print("--- Connecting to CockroachDB cluster... ---")
        engine = create_engine(DATABASE_URL)
        
        # SQL query to get the first 10 rows
        query = "SELECT * FROM model_training_data LIMIT 10"
        
        print(f"--- Fetching top 10 rows from 'model_training_data'... ---")
        # Use pandas to execute the query and load the result into a DataFrame
        df = pd.read_sql(query, engine)
        
        if df.empty:
            print("\nTable is empty or does not exist.")
        else:
            print("\n--- Table Contents ---")
            print(df)
            print("\n")

    except Exception as e:
        print(f"🔥 An error occurred: {e}")

if __name__ == "__main__":
    view_table_contents()