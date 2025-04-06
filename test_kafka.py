#!/usr/bin/env python3
import json
import logging
from kafka import KafkaConsumer, KafkaProducer
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('kafka_diagnostic')

def test_kafka_broker():
    try:
        logger.info("Testing Kafka broker connection at localhost:9092...")
        
        # Create a producer with a short timeout
        producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            request_timeout_ms=5000,
            connections_max_idle_ms=5000
        )
        
        # Attempt to fetch cluster metadata
        producer.flush(timeout=5)
        producer.close()
        
        logger.info("✅ Successfully connected to Kafka broker")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to connect to Kafka broker: {e}")
        return False

def test_diabetes_topic():
    try:
        logger.info("Testing existence of 'diabetes-data' topic...")
        
        consumer = KafkaConsumer(
            'diabetes-data',
            bootstrap_servers='localhost:9092',
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            group_id='test-group',
            consumer_timeout_ms=5000,
            request_timeout_ms=15000,  # Fix: Make request timeout > session timeout
            session_timeout_ms=10000,
            value_deserializer=lambda m: None if not m else json.loads(m.decode('utf-8'))
        )
        
        messages = consumer.poll(timeout_ms=5000)
        consumer.close()
        
        if not messages:
            logger.warning("⚠️ Topic 'diabetes-data' exists but has no messages")
        else:
            logger.info(f"✅ Topic 'diabetes-data' exists with {len(messages)} message partitions")
        
        return True
    except Exception as e:
        logger.error(f"❌ Error accessing 'diabetes-data' topic: {e}")
        return False

def send_test_message():
    try:
        logger.info("Attempting to send a test message to 'diabetes-data' topic...")
        
        producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        
        # Create a test record similar to your expected data format
        test_record = {
            "age": 45.0,
            "gender": "Male",
            "bmi": 28.5,
            "blood_glucose_level": 140.0,
            "HbA1c_level": 6.7,
            "hypertension": 0,
            "heart_disease": 0,
            "smoking_history": "never",
            "diabetes": 0,
            "timestamp": int(time.time())
        }
        
        # Send the message
        producer.send('diabetes-data', test_record)
        producer.flush()
        producer.close()
        
        logger.info("✅ Successfully sent test message to 'diabetes-data' topic")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to send test message: {e}")
        return False

def run_diagnostics():
    """Run all diagnostic tests"""
    logger.info("=== Starting Kafka Diagnostic Tests ===")
    
    # Test 1: Broker connection
    broker_ok = test_kafka_broker()
    if not broker_ok:
        logger.error("🛑 Broker connection failed - check Kafka service status")
        logger.info("Try: sudo systemctl status kafka")
        logger.info("Try: sudo systemctl start kafka")
        return False
    
    # Test 2: Topic existence
    topic_ok = test_diabetes_topic()
    if not topic_ok:
        logger.warning("⚠️ Topic issues detected - will try to create by sending a message")
        
    # Test 3: Send test message
    message_ok = send_test_message()
    if not message_ok:
        logger.error("🛑 Failed to send test message")
        return False
    
    # Test 4: Confirm receipt of test message
    logger.info("Waiting 3 seconds for message to propagate...")
    time.sleep(3)
    
    try:
        consumer = KafkaConsumer(
            'diabetes-data',
            bootstrap_servers='localhost:9092',
            auto_offset_reset='latest',
            enable_auto_commit=False,
            group_id='test-verification',
            consumer_timeout_ms=5000,
            request_timeout_ms=15000,
            session_timeout_ms=10000,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        
        # Poll for messages - should get our test message
        start_time = time.time()
        messages_received = False
        
        while time.time() - start_time < 10:  # Try for 10 seconds
            messages = consumer.poll(timeout_ms=2000)
            if messages:
                messages_received = True
                break
            time.sleep(0.5)
        
        consumer.close()
        
        if messages_received:
            logger.info("✅ Successfully received messages from 'diabetes-data' topic")
        else:
            logger.warning("⚠️ No messages received from 'diabetes-data' topic in verification step")
    except Exception as e:
        logger.error(f"❌ Error during verification: {e}")
    
    logger.info("=== Kafka Diagnostic Tests Complete ===")
    
    if broker_ok and (topic_ok or message_ok):
        logger.info("✅ Basic Kafka connectivity looks good")
        return True
    else:
        logger.error("🛑 Kafka setup has issues - see above for details")
        return False

if __name__ == "__main__":
    run_diagnostics()
