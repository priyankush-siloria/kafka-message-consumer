# Kafka Message Consumer Service

This Python service consumes messages from a Kafka topic named `Scenario-Execute` and processes them concurrently. The service is designed to handle long-running or CPU-intensive tasks in a way that ensures subsequent messages are processed while the current task is running.

## Key Features

- Connects to a Kafka cluster using environment variables.
- Consumes messages from the `Scenario-Execute` Kafka topic.
- Processes incoming messages concurrently, respecting the number of CPU cores available on the host machine.
- Simulates a long-running CPU-bound task to demonstrate concurrent message processing.

## Requirements

- Python 3.x
- Kafka cluster
- `pip` for managing dependencies

## Project Structure

The project includes the following files:

```
.
├── main.py               # Main script to consume and process messages
├── .env.example          # Example environment variable file for configuration
├── requirements.txt      # List of required dependencies
└── README.md             # Project documentation
```

## Setup Instructions

### Clone the repository (or create the necessary files):

```bash
git clone https://github.com/priyankush-siloria/kafka-message-consumer.git
cd kafka-message-consumer
```

### Install Dependencies:

Ensure you have Python 3.x installed, then use `pip` to install the required libraries.

```bash
pip install -r requirements.txt
```

### Configure the Environment Variables:

Copy the `.env.example` file to `.env` and update it with your Kafka server information:

```bash
cp .env.example .env
```

Example `.env` file:

```
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_GROUP_ID=test-group
KAFKA_TOPIC=Scenario-Execute
KAFKA_RESPONSE_TOPIC=Scenario-Execute-Response
```

### Run the Service:

Execute the `main.py` script to start consuming and processing messages.

```bash
python main.py
```

## Concurrency Approach

The service uses threads to handle message processing concurrently. Each message received from Kafka is processed in a separate thread to simulate parallel processing. The number of threads is dynamically adjusted to the available CPU cores on the host machine to avoid overwhelming the system.

### Python GIL Considerations:

Python's Global Interpreter Lock (GIL) affects true CPU-bound tasks when using threads. In this script, threading is used to handle long-running tasks concurrently, but Python's GIL may limit performance if tasks are CPU-bound. To achieve better parallelism for CPU-bound tasks, consider using multiprocessing or scaling the service across multiple machines.

## Simulated Long-Running Task

The service simulates a long-running task in the `handle_message` function by using `time.sleep(5)` to represent a CPU-intensive operation. This task demonstrates how the consumer can handle multiple messages concurrently without being blocked by a single long-running task.

## Challenges Faced and Solutions

### Challenge 1: Handling Concurrent Message Processing
One of the main challenges was ensuring that the service could handle multiple messages concurrently without overwhelming the system. The initial implementation faced issues with message processing delays, especially when multiple long-running tasks were triggered simultaneously.

**Solution:** 
To address this, I implemented threading to allow each message to be processed in its own thread. This approach helped distribute the workload across available CPU cores, improving the overall responsiveness of the service.

### Challenge 2: Message Decoding Errors
During testing, I encountered issues with decoding messages from Kafka, particularly when the message format was not as expected. This led to exceptions that could crash the consumer.

**Solution:** 
I added error handling in the `decode_message` function to catch `json.JSONDecodeError` exceptions. This ensured that the consumer could skip invalid messages without crashing, allowing it to continue processing valid messages.