import os
import json
import threading
import time

from kafka import KafkaConsumer
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Environment variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
KAFKA_GROUP_ID = os.getenv('KAFKA_GROUP_ID')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')

def handle_message(data):
    """
    Simulate a long-running task.
    """
    print(f"Processing message: {data}")
    try:
        time.sleep(5)
        print(f"Finished processing message: {data}")
    except Exception as e:
        print(f"Error processing message: {data}. Error: {e}")

def decode_message(message):
    """
    Decode message value from Kafka.
    """
    try:
        return json.loads(message.decode("utf-8")) if message else None
    except json.JSONDecodeError as e:
        print(f"Error decoding message: {message}. Error: {e}")
        return None

def receive_messages():
    """
    Consume messages from the Kafka topic and process them concurrently.
    """
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset='earliest',
        value_deserializer=decode_message
    )

    print(f"Consuming messages from Kafka topic: {KAFKA_TOPIC}")

    def worker_thread(message):
        if message.value:
            handle_message(message.value)
        else:
            print(f"Invalid message received from topic {KAFKA_TOPIC}. Skipping...")

    try:
        for message in consumer:
            print(f"Received message: {message}")
            thread = threading.Thread(target=worker_thread, args=(message,))
            thread.start()
    except KeyboardInterrupt:
        print(" --- Closing Kafka consumer by user interrupt ---")
    except Exception as e:
        print(f" Error consuming messages from Kafka: {e} ")
    finally:
        consumer.close()
        print(" Kafka Connection closed ")

if __name__ == "__main__":
    print("Connecting to Kafka...")
    receive_messages()
