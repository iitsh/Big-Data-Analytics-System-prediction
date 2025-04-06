#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, LongType
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml import PipelineModel
import signal
import sys
import os
import time

# Global variables for clean shutdown
spark = None
query = None

def signal_handler(sig, frame):
    print("\nShutting down gracefully...")
    if query:
        query.stop()
    if spark:
        spark.stop()
    sys.exit(0)

def create_spark_session():
    return (SparkSession.builder
            .appName("DiabetesStreamProcessor")
            .master("local[*]")
            .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint")
            .config("spark.hadoop.fs.defaultFS", "file:///")
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,org.postgresql:postgresql:42.2.23")
            .getOrCreate())

def define_schema():
    return StructType([
        StructField("gender", StringType(), True),
        StructField("age", DoubleType(), True),
        StructField("hypertension", IntegerType(), True),
        StructField("heart_disease", IntegerType(), True),
        StructField("smoking_history", StringType(), True),
        StructField("bmi", DoubleType(), True),
        StructField("HbA1c_level", DoubleType(), True),
        StructField("blood_glucose_level", DoubleType(), True),
        StructField("diabetes", IntegerType(), True),
        StructField("timestamp", LongType(), True)
    ])

def load_model(model_path):
    """Load the trained Random Forest model"""
    try:
        model = PipelineModel.load(model_path)
        print(f"Model loaded successfully from {model_path}")
        return model
    except Exception as e:
        print(f"Error loading model: {e}")
        return None

def predict_diabetes(df):
    """Legacy rule-based prediction (fallback method)"""
    return df.withColumn("predicted_diabetes",
        when((col("blood_glucose_level") > 140) & (col("HbA1c_level") > 6.5), 1)
        .when((col("blood_glucose_level") > 125) & (col("HbA1c_level") > 6.0) & (col("bmi") > 30), 1)
        .when(col("age") > 65, 1)
        .otherwise(0)
    )

def predict_with_model(df, model):
    """Predict diabetes using the trained model"""
    try:
        # Process categorical features
        for cat_col in ["gender", "smoking_history"]:
            indexer = StringIndexer(inputCol=cat_col, outputCol=f"{cat_col}_indexed", handleInvalid="keep")
            df = indexer.fit(df).transform(df)
        
        # Select features to match training data format
        feature_cols = ["age", "hypertension", "heart_disease", "bmi", 
                       "HbA1c_level", "blood_glucose_level", 
                       "gender_indexed", "smoking_history_indexed"]
        
        # Assemble features vector
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="keep")
        df_with_features = assembler.transform(df)
        
        # Apply model to make predictions
        predictions = model.transform(df_with_features)
        
        # Return with consistent column name
        return predictions.withColumnRenamed("prediction", "predicted_diabetes")
    except Exception as e:
        print(f"Error in model prediction: {e}")
        print("Falling back to rule-based prediction")
        return predict_diabetes(df)

def save_to_csv(df, batch_id):
    try:
        timestamp = int(time.time())
        output_dir = "diabetes_predictions_csv"
        os.makedirs(output_dir, exist_ok=True)

        csv_path = f"{output_dir}/batch_{batch_id}_{timestamp}"
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(csv_path)
        print(f"Results saved to CSV: {csv_path}")
        return True
    except Exception as e:
        print(f"Error saving to CSV: {e}")
        return False

def process_streaming_data(spark, kafka_server, topic, pg_config, model_path):
    # Load the Random Forest model
    model = load_model(model_path)

    # Read from Kafka
    kafka_df = (spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_server)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load())

    # Parse JSON data
    schema = define_schema()
    parsed_df = kafka_df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    # Add timestamp
    parsed_df = parsed_df.withColumn("processing_time", current_timestamp())

    # Process each batch
    def process_batch(batch_df, batch_id):
        try:
            count = batch_df.count()
            if count == 0:
                return

            print(f"\nProcessing batch {batch_id} with {count} records")

            # Apply prediction logic using model or fallback to rules
            if model is not None:
                predictions_df = predict_with_model(batch_df, model)
                print("Using trained Random Forest model for predictions")
            else:
                predictions_df = predict_diabetes(batch_df)
                print("Using fallback rule-based predictions")

            # Prepare results
            result_df = predictions_df.select(
                col("gender"),
                col("age"),
                col("hypertension"),
                col("heart_disease"),
                col("smoking_history"),
                col("bmi"),
                col("HbA1c_level"),
                col("blood_glucose_level"),
                col("diabetes").alias("actual_diabetes"),
                col("predicted_diabetes"),
                col("processing_time")
            )

            # Calculate accuracy
            correct_count = result_df.filter(col("actual_diabetes") == col("predicted_diabetes")).count()
            accuracy = correct_count / count if count > 0 else 0
            print(f"Batch accuracy: {accuracy:.2f} ({correct_count}/{count})")

            # Display sample predictions
            result_df.select("gender", "age", "actual_diabetes", "predicted_diabetes").show(5, False)

            # Try to save to PostgreSQL
            try:
                jdbc_url = f"jdbc:postgresql://{pg_config['host']}:{pg_config['port']}/{pg_config['database']}"

                properties = {
                    "user": pg_config['user'],
                    "password": pg_config['password'],
                    "driver": "org.postgresql.Driver"
                }

                # Test connection before saving
                test_connection = spark.read \
                    .format("jdbc") \
                    .option("url", jdbc_url) \
                    .option("dbtable", "information_schema.tables") \
                    .option("user", pg_config['user']) \
                    .option("password", pg_config['password']) \
                    .option("driver", "org.postgresql.Driver") \
                    .load()

                test_connection.take(1)  # Force evaluation

                # Save to PostgreSQL
                result_df.write \
                    .jdbc(url=jdbc_url,
                         table=pg_config['table'],
                         mode="append",
                         properties=properties)

                print(f"Results saved to PostgreSQL table: {pg_config['table']}")
            except Exception as e:
                print(f"Error saving to PostgreSQL: {e}")
                # Fallback to CSV storage
                save_to_csv(result_df, batch_id)

        except Exception as e:
            print(f"Error processing batch: {e}")

    # Start streaming with micro-batch processing
    return parsed_df.writeStream.foreachBatch(process_batch).start()

def main():
    # Set up signal handler for clean shutdown
    signal.signal(signal.SIGINT, signal_handler)

    global spark, query

    print("Starting Diabetes Stream Processor with Random Forest model")

    # PostgreSQL configuration
    pg_config = {
        'host': 'localhost',
        'port': '5432',
        'database': 'diabetes',
        'table': 'predictions',
        'user': 'your_user',
        'password': 'your_password'
    }

    # Initialize Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Path to the trained Random Forest model
    model_path = "randomforest_model_20250330_174432"

    try:
        # Start streaming
        query = process_streaming_data(
            spark=spark,
            kafka_server="localhost:9092",
            topic="diabetes-data",
            pg_config=pg_config,
            model_path=model_path
        )

        print("Streaming started. Waiting for data...\n")
        print("Press Ctrl+C to stop gracefully")

        # Wait for termination
        query.awaitTermination()

    except Exception as e:
        print(f"Error: {e}")
        if query:
            query.stop()
        if spark:
            spark.stop()

if __name__ == "__main__":
    main()
