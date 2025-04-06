#!/usr/bin/env python3
import json
import time
import glob
import pandas as pd
import numpy as np
from kafka import KafkaProducer
import sys
import os

def convert_to_serializable(value):
    """Convert numpy/pandas types to JSON serializable types"""
    if isinstance(value, (np.integer, np.int64)):
        return int(value)
    elif isinstance(value, (np.floating, np.float64)):
        return float(value)
    elif isinstance(value, np.ndarray):
        return value.tolist()
    elif pd.isna(value):
        return None
    return value

def main():
    # Load processed data
    data_path = "processed_diabetes_data_csv"
    csv_files = glob.glob(f"{data_path}/*.csv")
    if not csv_files:
        print(f"Error: No CSV files found in {data_path}")
        if os.path.exists("diabetes.csv"):
            print("Using original diabetes.csv file instead")
            csv_files = ["diabetes.csv"]
        else:
            sys.exit(1)
    
    # Load processed data
    df = pd.read_csv(csv_files[0])
    print(f"Loaded {len(df)} records from {csv_files[0]}")
    
    # Load original data to get categorical fields
    if os.path.exists("diabetes.csv"):
        print("Loading original dataset for gender and categorical data...")
        original_df = pd.read_csv("diabetes.csv")
        
        # Map categorical fields
        for col in ["gender", "smoking_history"]:
            if col not in df.columns or df[col].isna().all():
                if col in original_df.columns:
                    df[col] = original_df[col].values[:len(df)]
                    print(f"Added '{col}' from original dataset")
                else:
                    df[col] = "Female"  # Realistic default
    
    # Ensure all required fields exist
    required_cols = [
        "gender", "age", "hypertension", "heart_disease", "smoking_history",
        "bmi", "HbA1c_level", "blood_glucose_level", "diabetes"
    ]
    
    for col in required_cols:
        if col not in df.columns:
            print(f"Warning: Column '{col}' not found, adding defaults.")
            if col == "gender":
                df[col] = "Female"  # More realistic default
            elif col == "smoking_history":
                df[col] = "never"  # More realistic default
            else:
                df[col] = 0
    
    # Print sample data
    print("\nSample data to be sent:")
    sample = df[required_cols].head(2)
    print(sample.to_string())
    
    # Create Kafka producer
    try:
        producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        print("Connected to Kafka broker")
    except Exception as e:
        print(f"Error connecting to Kafka: {e}")
        sys.exit(1)
    
    # Stream data line by line
    total_records = len(df)
    print(f"Starting to send {total_records} records...")
    count = 0
    
    try:
        for idx, row in df.iterrows():
            # Convert row to dict with proper serialization
            record = {k: convert_to_serializable(v) for k, v in row.items()}
            
            # Add timestamp for Kafka
            record["timestamp"] = int(time.time())
            
            # Send to Kafka
            producer.send('diabetes-data', record)
            producer.flush()
            
            # Update progress
            count += 1
            print(f"{count}/{total_records}", end="\r")
            
            # Add delay to simulate streaming
            time.sleep(0.1)
        
        print(f"\nCompleted sending {count} records")
    except KeyboardInterrupt:
        print(f"\nStopped after sending {count} records")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
