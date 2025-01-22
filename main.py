import os
import json
import logging
import time
from concurrent.futures import ProcessPoolExecutor
from kafka import KafkaConsumer
from dotenv import load_dotenv
import multiprocessing

# Load environment variables from .env
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Environment variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
KAFKA_GROUP_ID = os.getenv('KAFKA_GROUP_ID')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')

def process_message(data):
    """
    Simulate a long-running or CPU-intensive task.
    """
    logging.info(f"Processing message: {data}")
    try:
        # Simulate a CPU-bound task
        time.sleep(5)  # Simulate a blocking task
        logging.info(f"Finished processing message: {data}")
    except Exception as e:
        logging.error(f"Error processing message: {data}. Error: {e}")

def deserialize_message(message):
    """
    Deserialize message value from Kafka.
    """
    try:
        return json.loads(message.decode("utf-8")) if message else None
    except json.JSONDecodeError as e:
        logging.error(f"Error decoding message: {message}. Error: {e}")
        return None

def consume_messages():
    """
    Consume messages from the Kafka topic and process them concurrently.
    """
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset='earliest',
        value_deserializer=deserialize_message
    )

    # Determine the number of CPU cores and configure workers accordingly
    max_workers = multiprocessing.cpu_count()
    logging.info(f"Using {max_workers} workers for processing")

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        try:
            logging.info(f"Consuming messages from Kafka topic: {KAFKA_TOPIC}")
            for message in consumer:
                logging.debug(f"Received raw message: {message.value}")
                
                if message.value:  # Ensure that the message is not empty
                    executor.submit(process_message, message.value)
                else:
                    logging.warning(f"Empty message received from topic {KAFKA_TOPIC}. Skipping...")
        
        except KeyboardInterrupt:
            logging.info("Shutting down Kafka consumer gracefully.")
        except Exception as e:
            logging.error(f"Error consuming messages from Kafka: {e}")
        finally:
            consumer.close()  # Close the consumer after processing
            logging.info("Kafka consumer closed.")

if __name__ == "__main__":
    logging.info("Starting Kafka consumer service...")
    consume_messages()
