"""
Main ETL Pipeline for Transaction Processing

This script runs continuously, generating fake transactions every minute
and processing them through a data pipeline.

TODO: Complete the following functions:
1. clean_data() - Clean and validate the raw transaction data
2. detect_suspicious_transactions() - Identify potentially fraudulent transactions
"""

import time
import pandas as pd
from datetime import datetime
from pathlib import Path
from scripts.generate_transactions import generate_transactions


# Configuration
TRANSACTIONS_FOLDER = Path("./transactions")
PROCESSED_FOLDER = Path("./processed")
SUSPICIOUS_FOLDER = Path("./suspicious")
INTERVAL_SECONDS = 60  # Generate transactions every 1 minute
TRANSACTIONS_PER_BATCH = 100  # Number of transactions to generate each time


def setup_folders():
    """Create necessary folders if they don't exist"""
    TRANSACTIONS_FOLDER.mkdir(exist_ok=True)
    PROCESSED_FOLDER.mkdir(exist_ok=True)
    SUSPICIOUS_FOLDER.mkdir(exist_ok=True)
    print(f"Folders initialized:")
    print(f"  - Data Lake: {TRANSACTIONS_FOLDER}")
    print(f"  - Processed: {PROCESSED_FOLDER}")
    print(f"  - Suspicious: {SUSPICIOUS_FOLDER}")


def generate_batch():
    """Generate a batch of fake transactions and save to data lake"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = TRANSACTIONS_FOLDER / f"transactions_{timestamp}.csv"

    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Generating {TRANSACTIONS_PER_BATCH} transactions...")
    df = generate_transactions(TRANSACTIONS_PER_BATCH)
    df.to_csv(filename, index=False)
    print(f"Saved to: {filename}")

    return filename


def clean_data(df):
    """
    TODO: Implement data cleaning logic

    Clean and validate the transaction data. Consider:
    - Handling missing values
    - Removing duplicates
    - Data type validation
    - Standardizing formats
    - Handling outliers

    Args:
        df (pd.DataFrame): Raw transaction data

    Returns:
        pd.DataFrame: Cleaned transaction data
    """
    # YOUR CODE HERE
    # Example structure:
    # df_clean = df.copy()
    # ... your cleaning logic ...
    # return df_clean
    df_clean = df.copy()

    # 1. Drop duplicates and remove rows with missing settlement date. Invalid transactions
    df_clean = df_clean.drop_duplicates()
    df_clean.dropna(subset=['settlement_date'], inplace=True) 
    # 2. N/A and NULLS
    df_clean['amount'] = pd.to_numeric(df_clean['amount'], errors='coerce')
    df_clean = df_clean.dropna(subset=['transaction_id', 'user_id']) # IDs are required
    df_clean['amount'] = df_clean['amount'].fillna(df_clean['amount'].median())

    # 3. Formatting
    if 'timestamp' in df_clean.columns:
        df_clean['timestamp'] = pd.to_datetime(df_clean['timestamp'])
    
    if 'country_code' in df_clean.columns:
        df_clean['country_code'] = df_clean['country_code'].str.upper().str.strip()

    # 4. Outliers (IQR)
    Q1 = df_clean['amount'].quantile(0.25)
    Q3 = df_clean['amount'].quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = 0 
    upper_bound = Q3 + 1.5 * IQR
    
    # Format amount
    df_clean['amount'] = df_clean['amount'].astype(float)

    return df_clean

def detect_suspicious_transactions(df_clean):
    """
    TODO: Implement fraud detection logic

    Identify suspicious transactions based on various criteria. Consider:
    - Unusually high amounts
    - Multiple failed attempts
    - High-risk countries or merchants
    - Unusual transaction patterns
    - Time-based anomalies
    - Multiple transactions in short time

    Args:
        df (pd.DataFrame): Cleaned transaction data

    Returns:
        tuple: (normal_df, suspicious_df) - DataFrames split by suspicion status
    """
    if df_clean.empty:
        return df_clean, df_clean

    
    df_work = df_clean.copy()
    
    # Detecting Suspicious Transactions
    df_work['is_suspicious'] = False

    # Unusually high amounts
    limit = 5000 
    df_work.loc[df_work['amount'] > limit, 'is_suspicious'] = True

    # Multiple failed attempts
    if 'status' in df_work.columns and 'user_id' in df_work.columns:
        # Number of declined transactions
        df_work['declined_count'] = df_work[df_work['status'] == 'declined'].groupby('user_id')['transaction_id'].transform('count')
        df_work['declined_count'] = df_work['declined_count'].fillna(0)
        
        # Flag transactions with more than 2 declined attempts
        df_work.loc[df_work['declined_count'] > 2, 'is_suspicious'] = True

    # Split dataframes
    suspicious_df = df_work[df_work['is_suspicious'] == True].copy()
    normal_df = df_work[df_work['is_suspicious'] == False].copy()

    # Drop unnecessary columns
    columns_to_drop = ['is_suspicious', 'declined_count']
    suspicious_df = suspicious_df.drop(columns=columns_to_drop, errors='ignore')
    normal_df = normal_df.drop(columns=columns_to_drop, errors='ignore')

    return normal_df, suspicious_df


def process_batch(raw_file):
    """
    Process a batch of transactions through the ETL pipeline

    Args:
        raw_file (Path): Path to the raw transaction CSV file
    """
    try:
        # Read raw data from data lake
        print(f"Reading data from: {raw_file}")
        df_raw = pd.read_csv(raw_file)
        print(f"Loaded {len(df_raw)} transactions")

        # Step 1: Clean the data
        print("Cleaning data...")
        df_clean = clean_data(df_raw)
        print(f"Cleaned {len(df_clean)} transactions")

        # Step 2: Detect suspicious transactions
        print("Detecting suspicious transactions...")
        df_normal, df_suspicious = detect_suspicious_transactions(df_clean)
        print(f"Found {len(df_suspicious)} suspicious transactions")
        print(f"Found {len(df_normal)} normal transactions")

        # Save processed results
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        if len(df_normal) > 0:
            normal_file = PROCESSED_FOLDER / f"processed_{timestamp}.csv"
            df_normal.to_csv(normal_file, index=False)
            print(f"Saved normal transactions to: {normal_file}")

        if len(df_suspicious) > 0:
            suspicious_file = SUSPICIOUS_FOLDER / f"suspicious_{timestamp}.csv"
            df_suspicious.to_csv(suspicious_file, index=False)
            print(f"WARNING: Saved suspicious transactions to: {suspicious_file}")

        print(f"Batch processing completed successfully")

    except NotImplementedError as e:
        print(f"WARNING: Skipping processing: {e}")
    except Exception as e:
        print(f"ERROR: Error processing batch: {e}")


def main():
    """Main loop - generates and processes transactions every minute"""
    print("="*60)
    print("Transaction Processing Pipeline")
    print("="*60)

    setup_folders()

    print(f"\nStarting continuous processing (every {INTERVAL_SECONDS} seconds)")
    print("Press Ctrl+C to stop\n")

    batch_count = 0

    try:
        while True:
            batch_count += 1
            print(f"\n{'='*60}")
            print(f"BATCH #{batch_count}")
            print(f"{'='*60}")

            # Generate new transactions
            raw_file = generate_batch()

            # Process the batch
            process_batch(raw_file)

            # Wait for next interval
            print(f"\nWaiting {INTERVAL_SECONDS} seconds until next batch...")
            time.sleep(INTERVAL_SECONDS)

    except KeyboardInterrupt:
        print("\n\nPipeline stopped by user")
        print(f"Total batches processed: {batch_count}")


if __name__ == "__main__":
    main()
