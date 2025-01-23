import os
import json
import logging
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from kafka import KafkaConsumer, KafkaProducer
from dotenv import load_dotenv
from collections import defaultdict
from datetime import datetime, timedelta


# Load environment variables from .env
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Environment variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
KAFKA_GROUP_ID = os.getenv('KAFKA_GROUP_ID')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')
KAFKA_RESPONSE_TOPIC = os.getenv('KAFKA_RESPONSE_TOPIC', 'Scenario-Execute-Response')


def initialize_producer():
    """
    Initialize and return a KafkaProducer instance.
    """
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )


def send_message_to_kafka(producer, topic, payload):
    """
    Produce a message to the specified Kafka topic.
    """
    try:
        producer.send(topic, value=payload)
        producer.flush()
        logging.info(f"Message sent to {topic}: {payload}")
    except Exception as e:
        logging.error(f"Error sending message to {topic}: {e}")


def process_message(data):
    """
    Process the message by aggregating weekly data and sending the response to Kafka.
    """
    try:
        logging.info("Started processing message.")

        # Aggregate weekly data
        aggregated_data = aggregate_weekly_data(data["weekly"])

        # Prepare and send response payload
        response_payload = {
            "event": "rollup",
            "organizationId": data["organizationId"],
            "week_start": data["week_start"],
            "weekly": aggregated_data
        }

        logging.info("Aggregated data successfully. Preparing to send response to Kafka.")
        producer = initialize_producer()
        send_message_to_kafka(producer, KAFKA_RESPONSE_TOPIC, response_payload)
        producer.close()
        logging.info("Message processed and sent successfully.")
    except Exception as e:
        logging.error(f"Error processing message: {e}")


def calculate_average(values):
    return sum(values) / len(values) if values else 0

# Helper function to find the start of the week (Friday)
def get_start_of_week(date_str):
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    start_of_week = date_obj - timedelta(days=(date_obj.weekday() + 3) % 7)  # Friday is the start of the week
    return start_of_week

def aggregate_weekly_data(weekly_data):

    """
    Aggregates weekly data by Contact Type, Staff Type, and Call Center.
    """
    dict = {}

    for entry in weekly_data:
        key = f"{entry['Contact Type']}_{entry['Staff Type']}_{entry['Call Center']}"
        if key not in dict:
            dict[key] = []
        else:
            dict[key].append(entry)

    result_data = {}
    for key, value in dict.items():
        len_of_value = len(value)
        aggregated_data = defaultdict(lambda: {
            "Volume": 0,
            "Abandons": 0.0,
            "Top Line Agents (FTE)": 0.0,
            "Base AHT": 0.0,
            "Handled Threshold": 0.0,
            "Service Level (X Seconds)": 0.0,
            "Acceptable Wait Time": 0.0,
            "Total Queue Time": 0.0,
            "Base AHT Sum": 0.0,
            "Service Level Sum": 0.0
        })

        for entry in value:
            # Get the start of the week for the date
            week_start = get_start_of_week(entry["Week"])
            
            # Add the data to the corresponding week
            aggregated_data[str(week_start)]["Volume"] += entry["Volume"]
            aggregated_data[str(week_start)]["Abandons"] += entry["Abandons"]
            aggregated_data[str(week_start)]["Top Line Agents (FTE)"] += entry["Top Line Agents (FTE)"]
            aggregated_data[str(week_start)]["Base AHT Sum"] += entry["Base AHT"]
            aggregated_data[str(week_start)]["Handled Threshold"] += entry["Handled Threshold"]
            aggregated_data[str(week_start)]["Service Level Sum"] += entry["Service Level (X Seconds)"]
            aggregated_data[str(week_start)]["Acceptable Wait Time"] += entry["Acceptable Wait Time"]
            aggregated_data[str(week_start)]["Total Queue Time"] += entry["Total Queue Time"]

        result = []
        for week_start, values in aggregated_data.items():
            result.append({
                "Week": str(week_start),
                "Volume": values["Volume"],
                "Abandons": values["Abandons"],
                "Top Line Agents (FTE)": values["Top Line Agents (FTE)"],
                "Base AHT": values["Base AHT Sum"] / len_of_value,
                "Handled Threshold": values["Handled Threshold"],
                "Service Level (X Seconds)": values["Service Level Sum"] / len_of_value,
                "Acceptable Wait Time": values["Acceptable Wait Time"] / len_of_value,
                "Total Queue Time": values["Total Queue Time"]
            })

        result_data[key] = result

    return result_data

def deserialize_message(message):
    """
    Deserialize Kafka message value into Python dictionary.
    """
    try:
        return json.loads(message.decode("utf-8")) if message else None
    except json.JSONDecodeError as e:
        logging.error(f"Error decoding message: {message}. Error: {e}")
        return None


def consume_messages():
    """
    Consume messages from Kafka topic and process them concurrently.
    """
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset='earliest',
        value_deserializer=deserialize_message
    )

    max_workers = multiprocessing.cpu_count()
    logging.info(f"Using {max_workers} workers for processing")

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        try:
            logging.info(f"Consuming messages from Kafka topic: {KAFKA_TOPIC}")
            for message in consumer:
                logging.debug(f"Received raw message: {message.value}")
                
                if message.value:
                    executor.submit(process_message, message.value)
                else:
                    logging.warning(f"Empty message received from topic {KAFKA_TOPIC}. Skipping...")

        except KeyboardInterrupt:
            logging.info("Shutting down Kafka consumer gracefully.")
        except Exception as e:
            logging.error(f"Error consuming messages from Kafka: {e}")
        finally:
            consumer.close()
            logging.info("Kafka consumer closed.")


if __name__ == "__main__":
    logging.info("Starting Kafka consumer service...")
    consume_messages()
