#!/usr/bin/env python3
import psycopg2
import argparse
import sys

def clear_predictions_table(truncate=False):
    
    conn = None
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            dbname="diabetes",
            user="your_username",
            password="your_password",
            host="localhost"
        )
        conn.autocommit = False  # Ensure transaction safety
        cursor = conn.cursor()
        
        # Get count before deletion
        cursor.execute("SELECT COUNT(*) FROM predictions")
        count_before = cursor.fetchone()[0]
        
        # Clear the table
        if truncate:
            cursor.execute("TRUNCATE TABLE predictions")
            print(f"Table 'predictions' truncated. Removed {count_before} records.")
        else:
            cursor.execute("DELETE FROM predictions")
            print(f"Deleted {count_before} records from 'predictions' table.")
        
        # Commit the transaction
        conn.commit()
        
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Error: {e}")
        sys.exit(1)
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Clear the predictions table in the diabetes database")
    parser.add_argument("--truncate", action="store_true", 
                        help="Use TRUNCATE instead of DELETE (faster but no transaction safety)")
    args = parser.parse_args()
    
    clear_predictions_table(args.truncate)
