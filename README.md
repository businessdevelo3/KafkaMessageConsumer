# Kafka Consumer Service

This service consumes messages from a Kafka topic named `Scenario-Execute` and processes them concurrently.

## Installation

1. Clone the repository

```bash
git clone https://github.com/businessdevelo3/KafkaMessageConsumer.git
cd KafkaMessageConsumer
```

2. Install the required dependencies:

   ```bash
   pip install -r requirements.txt
   ```

3. Create a `.env` file based on the `.env.example` and fill in the necessary Kafka configuration.

## Configuration

- `KAFKA_BOOTSTRAP_SERVERS`: Kafka server addresses.
- `KAFKA_GROUP_ID`: Consumer group ID.
- `KAFKA_TOPIC`: Topic to consume messages from.
- `KAFKA_RESPONSE_TOPIC`: Topic to send responses to (default: `Scenario-Execute-Response`).

## Running the Service

```bash
python main.py
```

## Concurrency Approach

This service uses `ProcessPoolExecutor` to process messages concurrently, leveraging the number of CPU cores available on the host machine. This approach is chosen to bypass Python's Global Interpreter Lock (GIL), which can be a bottleneck for CPU-bound tasks when using threads.

## Challenges Faced

One of the main challenges encountered during the development of this service was the request size limit imposed by Kafka. The maximum message size allowed by Kafka is typically 1 MB, but in this case, we set a custom limit of 10 MB to accommodate larger payloads. This required careful handling of message serialization and deserialization to ensure that the data was processed efficiently without exceeding the limit.

To overcome this challenge, I implemented checks to validate the size of incoming messages before processing them. If a message exceeds the specified size, it is logged as an error, and processing is skipped for that message. This ensures that the service remains robust and does not crash due to oversized payloads.

## Limitations

- The use of `ProcessPoolExecutor` is suitable for CPU-bound tasks but may have overhead due to process creation.
- For truly CPU-bound tasks, using processes is more efficient than threads due to the GIL.
- In a production environment, consider scaling horizontally by deploying multiple instances of this service.

## Simulated Blocking Task

The service simulates a blocking task using a `time.sleep(5)` call to demonstrate concurrency. Replace this with actual CPU-intensive logic as needed.
