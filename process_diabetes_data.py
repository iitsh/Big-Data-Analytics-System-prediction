#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.sql.functions import col, when, isnan, isnull
import os
import sys

# Green color for terminal output
GREEN = '\033[32m'
RESET = '\033[0m'

# Create Spark session with explicit LOCAL file system configuration
spark = SparkSession.builder \
    .appName("DiabetesDataProcessing") \
    .master("local[*]") \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .getOrCreate()

print(f"{GREEN}[INFO] Spark session created successfully{RESET}")

# Define paths - use current directory 
current_dir = os.getcwd()
input_file = os.path.join(current_dir, "diabetes.csv")
output_dir_parquet = os.path.join(current_dir, "processed_diabetes_data", "diabetes_processed")
output_dir_csv = os.path.join(current_dir, "processed_diabetes_data_csv")

print(f"{GREEN}[INFO] Loading diabetes data from {input_file}{RESET}")

# Check if file exists
if not os.path.exists(input_file):
    print(f"Error: File {input_file} does not exist!")
    sys.exit(1)

# Load the diabetes dataset
df = spark.read.csv(input_file, header=True, inferSchema=True)
print(f"{GREEN}[INFO] Dataset loaded with {df.count()} rows and {len(df.columns)} columns{RESET}")

# Check for missing values
for column in df.columns:
    missing_count = df.filter(isnull(col(column)) | isnan(col(column))).count()
    if missing_count > 0:
        print(f"{GREEN}[INFO] Found {missing_count} missing values in column {column}{RESET}")
        # Fill missing values
        if column in ["gender", "smoking_history"]:
            mode_val = df.groupBy(column).count().orderBy("count", ascending=False).first()[0]
            df = df.fillna(mode_val, subset=[column])
        else:
            mean_val = df.select(column).summary("mean").collect()[0][1]
            df = df.fillna(mean_val, subset=[column])

# Handle outliers in numerical columns
numerical_cols = ["age", "bmi", "HbA1c_level", "blood_glucose_level"]
for col_name in numerical_cols:
    quantiles = df.approxQuantile(col_name, [0.25, 0.75], 0.05)
    q1, q3 = quantiles[0], quantiles[1]
    iqr = q3 - q1
    
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr
    
    df = df.withColumn(
        col_name,
        when((col(col_name) < lower_bound), lower_bound)
        .when((col(col_name) > upper_bound), upper_bound)
        .otherwise(col(col_name))
    )

# Feature engineering
print(f"{GREEN}[INFO] Creating features{RESET}")
categorical_cols = ["gender", "smoking_history"]
numerical_cols = ["age", "hypertension", "heart_disease", "bmi", "HbA1c_level", "blood_glucose_level"]
label_col = "diabetes"

# Index categorical columns
indexed_cols = []
for c in categorical_cols:
    indexed_col = f"{c}_indexed"
    indexer = StringIndexer(inputCol=c, outputCol=indexed_col, handleInvalid="keep")
    df = indexer.fit(df).transform(df)
    indexed_cols.append(indexed_col)

# Create feature vector
assembler = VectorAssembler(inputCols=numerical_cols + indexed_cols, outputCol="features")
final_df = assembler.transform(df)

# Create output directories
os.makedirs(os.path.dirname(output_dir_parquet), exist_ok=True)
os.makedirs(output_dir_csv, exist_ok=True)

# Save parquet data - explicitly use save mode and repartition to 1
print(f"{GREEN}[INFO] Writing parquet file to {output_dir_parquet}{RESET}")
final_df.select("features", label_col).repartition(1).write.mode("overwrite").format("parquet").save(output_dir_parquet)

# Save CSV data - explicitly use save mode and repartition to 1
print(f"{GREEN}[INFO] Writing CSV file to {output_dir_csv}{RESET}")
df.select(*numerical_cols, *indexed_cols, label_col).repartition(1).write.mode("overwrite").format("csv").option("header", "true").save(output_dir_csv)

# Verify data is written successfully
print(f"{GREEN}[INFO] Verifying output directories{RESET}")
if os.path.exists(output_dir_parquet) and os.listdir(output_dir_parquet):
    print(f"{GREEN}[SUCCESS] Parquet files successfully written{RESET}")
else:
    print(f"Warning: No files found in {output_dir_parquet}")

if os.path.exists(output_dir_csv) and os.listdir(output_dir_csv):
    print(f"{GREEN}[SUCCESS] CSV files successfully written{RESET}")
else:
    print(f"Warning: No files found in {output_dir_csv}")

print(f"{GREEN}[SUCCESS] Data processing completed!{RESET}")

# Stop Spark session
spark.stop()
