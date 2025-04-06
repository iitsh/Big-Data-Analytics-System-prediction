#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.ml.classification import RandomForestClassifier, LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.sql.functions import udf, col
from pyspark.sql.types import DoubleType
import os
import datetime

# Terminal colors
GREEN = '\033[32m'
RESET = '\033[0m'

# Initialize Spark
spark = SparkSession.builder \
    .appName("DiabetesModelComparison") \
    .master("local[*]") \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .getOrCreate()

print(f"{GREEN}[INFO] Loading processed diabetes data{RESET}")
current_dir = os.getcwd()
data_path = os.path.join(current_dir, "processed_diabetes_data/diabetes_processed")
processed_data = spark.read.parquet(data_path)

# Split data with fixed seed for consistent comparison
train_data, test_data = processed_data.randomSplit([0.8, 0.2], seed=42)
print(f"{GREEN}[INFO] Training data: {train_data.count()} rows, Test data: {test_data.count()} rows{RESET}")

# Define evaluation function
def evaluate_model(predictions, model_name):
    binary_evaluator = BinaryClassificationEvaluator(
        labelCol="diabetes", 
        rawPredictionCol="rawPrediction", 
        metricName="areaUnderROC"
    )
    multi_evaluator = MulticlassClassificationEvaluator(labelCol="diabetes", predictionCol="prediction")
    
    metrics = {
        "model": model_name,
        "accuracy": multi_evaluator.setMetricName("accuracy").evaluate(predictions),
        "auc": binary_evaluator.evaluate(predictions),
        "precision": multi_evaluator.setMetricName("weightedPrecision").evaluate(predictions),
        "recall": multi_evaluator.setMetricName("weightedRecall").evaluate(predictions),
        "f1": multi_evaluator.setMetricName("f1").evaluate(predictions)
    }
    return metrics

# Extract probability for CSV export
@udf(returnType=DoubleType())
def extract_diabetes_probability(v):
    return float(v[1]) if len(v) > 1 else 0.0

# Train and evaluate models
models = []
metrics = []

# 1. Random Forest
print(f"{GREEN}[INFO] Training Random Forest model{RESET}")
rf = RandomForestClassifier(
    labelCol="diabetes",
    featuresCol="features",
    numTrees=100,
    maxDepth=10,
    seed=42
)
rf_model = rf.fit(train_data)
rf_predictions = rf_model.transform(test_data)
rf_metrics = evaluate_model(rf_predictions, "RandomForest")
metrics.append(rf_metrics)
models.append(("RandomForest", rf_model))

# 2. Logistic Regression
print(f"{GREEN}[INFO] Training Logistic Regression model{RESET}")
lr = LogisticRegression(
    labelCol="diabetes",
    featuresCol="features",
    maxIter=20,
    regParam=0.1
)
lr_model = lr.fit(train_data)
lr_predictions = lr_model.transform(test_data)
lr_metrics = evaluate_model(lr_predictions, "LogisticRegression")
metrics.append(lr_metrics)
models.append(("LogisticRegression", lr_model))

# Create timestamp for model saving
timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

# Save models
for model_name, model in models:
    model_dir = f"{model_name.lower()}_model_{timestamp}"
    os.makedirs(model_dir, exist_ok=True)
    model.write().overwrite().save(model_dir)
    print(f"{GREEN}[INFO] Saved {model_name} model to {model_dir}{RESET}")

# Determine best model
accuracy_metric = "accuracy"  # Choose primary metric for comparison
best_model_index = max(range(len(metrics)), key=lambda i: metrics[i][accuracy_metric])
best_model_name = metrics[best_model_index]["model"]

# Save best model predictions
best_predictions = models[best_model_index][1].transform(test_data)
best_predictions = best_predictions.withColumn(
    "diabetes_probability", 
    extract_diabetes_probability(col("probability"))
)

predictions_path = "best_model_predictions_csv"
os.makedirs(predictions_path, exist_ok=True)
best_predictions.select(
    "diabetes", 
    "prediction", 
    "diabetes_probability"
).write.mode("overwrite").option("header", "true").csv(predictions_path)

# Display comparison results
print(f"\n{GREEN}======= MODEL COMPARISON RESULTS ======={RESET}")
metric_names = ["accuracy", "auc", "precision", "recall", "f1"]
print(f"{'Model':<20} | {'Accuracy':<10} | {'AUC':<10} | {'Precision':<10} | {'Recall':<10} | {'F1 Score':<10}")
print(f"{'-'*20} | {'-'*10} | {'-'*10} | {'-'*10} | {'-'*10} | {'-'*10}")

for m in metrics:
    values = [f"{m[metric]:.4f}" for metric in metric_names]
    model_name = m["model"]
    best_marker = "* BEST *" if model_name == best_model_name else ""
    print(f"{model_name:<20} | {values[0]:<10} | {values[1]:<10} | {values[2]:<10} | {values[3]:<10} | {values[4]:<10} {best_marker}")

print(f"\n{GREEN}[RESULT] Best model: {best_model_name} with accuracy {metrics[best_model_index]['accuracy']:.4f}{RESET}")
print(f"{GREEN}[INFO] Best model predictions saved to {predictions_path}{RESET}")

spark.stop()
