# Big Data System for Pandemic Prediction

A massive data processing architecture for real-time predictive analysis of pandemics, implemented with Spark, Kafka, and associated technologies.

## Overview

This project implements a complete Big Data architecture for predicting and monitoring pandemics. The system uses a set of modern technologies (Spark, Spark Streaming, Kafka) to form a processing pipeline capable of collecting, storing, processing, and visualizing large volumes of health data in real-time.

## Architecture

The data pipeline is built around five main components:

1. **Data Collection** - Kafka broker for real-time data stream management
2. **Storage** - PostgreSQL database for persistent storage of predictions
3. **Processing** - Spark for data cleaning and preparation
4. **Modeling** - Spark MLlib for training predictive models (Random Forest, Logistic Regression)
5. **Visualization** - Interactive dashboards via Streamlit and Grafana

## Main Components

```
├── process_diabetes_data.py  # Data preprocessing
├── train_diabetes_model.py   # Predictive model training
├── test_kafka.py             # Kafka system diagnostics
├── diabetes_consumer.py      # Spark Streaming consumer for real-time predictions
├── diabetes_producer.py      # Test data producer
├── dashboard.py              # Streamlit visualization interface
└── clear_db.py               # Database cleanup utility
```

## Requirements

- Apache Spark 3.x
- Apache Kafka
- PostgreSQL
- Python 3.8+
- Python libraries: pyspark, kafka-python, streamlit, psycopg2, pandas, numpy

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/iitsh/Big-Data-Analytics-System-prediction.git
   cd Big-Data-Analytics-System-prediction
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Configure Kafka:
   ```bash
   # Start Zookeeper server
   bin/zookeeper-server-start.sh config/zookeeper.properties
   
   # Start Kafka server
   bin/kafka-server-start.sh config/server.properties
   
   # Create necessary topic
   bin/kafka-topics.sh --create --topic diabetes-data --bootstrap-server localhost:9092
   ```

4. Configure PostgreSQL:
   ```bash
   # Create database
   createdb pandemia_prediction
   ```

## Usage

1. Preprocess data:
   ```bash
   spark-submit process_diabetes_data.py
   ```

2. Train models:
   ```bash
   spark-submit train_diabetes_model.py
   ```

3. Test Kafka configuration:
   ```bash
   python3 test_kafka.py
   ```

4. Launch Spark Streaming consumer:
   ```bash
   python3 diabetes_consumer.py
   ```

5. Launch dashboard:
   ```bash
   streamlit run dashboard.py
   ```

6. To simulate a real-time data stream:
   ```bash
   python3 diabetes_producer.py
   ```

## Key Features

- **Robust data processing**: Cleaning, handling missing and outlier values
- **Predictive modeling**: Model comparison (Random Forest, Logistic Regression)
- **Real-time analysis**: Kafka stream consumption for instant predictions
- **Advanced monitoring**: Performance tracking and trend visualization
- **Data persistence**: Secure storage in PostgreSQL with CSV backup

## Results

The system achieves 95% accuracy with Random Forest and 90% with Logistic Regression for diabetes prediction, demonstrating that the Big Data approach can be effectively applied to pandemic forecasting.

## Future Perspectives

- Extension to other types of diseases
- Integration of geospatial data
- Performance optimization for even larger volumes
- Large-scale deployment on distributed clusters

