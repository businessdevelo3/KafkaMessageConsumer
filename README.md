# Kafka Consumer Service

This service consumes messages from a Kafka topic named `Scenario-Execute` and processes them concurrently.

## Installation

1. Clone the repository.
2. Install the required dependencies:

   ```bash
   pip install -r requirements.txt
   ```

3. Create a `.env` file based on the `.env.example` and fill in the necessary Kafka configuration.

## Configuration

- `KAFKA_BOOTSTRAP_SERVERS`: Kafka server addresses.
- `KAFKA_GROUP_ID`: Consumer group ID.
- `KAFKA_TOPIC`: Topic to consume messages from.

## Running the Service

```bash
python main.py
```

## Concurrency Approach

This service uses `ProcessPoolExecutor` to process messages concurrently, leveraging the number of CPU cores available on the host machine. This approach is chosen to bypass Python's Global Interpreter Lock (GIL), which can be a bottleneck for CPU-bound tasks when using threads.

## Limitations

- The use of `ProcessPoolExecutor` is suitable for CPU-bound tasks but may have overhead due to process creation.
- For truly CPU-bound tasks, using processes is more efficient than threads due to the GIL.
- In a production environment, consider scaling horizontally by deploying multiple instances of this service.

## Simulated Blocking Task

The service simulates a blocking task using a `time.sleep(5)` call to demonstrate concurrency. Replace this with actual CPU-intensive logic as needed.
