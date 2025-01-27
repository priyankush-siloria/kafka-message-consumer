import os
import json
import threading
import time
from kafka import KafkaConsumer, KafkaProducer
from dotenv import load_dotenv
from collections import defaultdict
from datetime import datetime, timedelta

# Load environment variables from .env
load_dotenv()

# Environment variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
KAFKA_RESPONSE_TOPIC = os.getenv("KAFKA_RESPONSE_TOPIC")


def produce_response_message(topic, payload):
    """
    Initialize a KafkaProducer instance and produce a message to the specified Kafka topic.
    """
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    try:
        producer.send(topic, value=payload)
        producer.flush()
        print(f" --- Message sent to {topic} ---")
    except Exception as e:
        print(f" --- Error sending message to {topic} ---")
    finally:
        producer.close()


def decode_message(message):
    """
    Decode message value from Kafka.
    """
    try:
        return json.loads(message.decode("utf-8")) if message else None
    except json.JSONDecodeError as e:
        print(f"Error decoding message: {message}. Error: {e}")
        return None


def extract_weekly_data(data):
    """
    Aggregates weekly data by Contact Type, Staff Type, and Call Center.
    """
    dict = {}

    for entry in data:
        key = f"{entry['Contact Type']}_{entry['Staff Type']}_{entry['Call Center']}"
        if key not in dict:
            dict[key] = []
        else:
            dict[key].append(entry)

    result_data = {}
    for key, value in dict.items():
        len_of_value = len(value)
        aggregated_data = defaultdict(
            lambda: {
                "Volume": 0,
                "Abandons": 0.0,
                "Top Line Agents (FTE)": 0.0,
                "Base AHT": 0.0,
                "Handled Threshold": 0.0,
                "Service Level (X Seconds)": 0.0,
                "Acceptable Wait Time": 0.0,
                "Total Queue Time": 0.0,
                "Base AHT Sum": 0.0,
                "Service Level Sum": 0.0,
            }
        )

        for entry in value:
            date_obj = datetime.strptime(entry["Week"], "%Y-%m-%d")
            week_start = (date_obj - timedelta(days=(date_obj.weekday() + 3) % 7)).date()

            aggregated_data[str(week_start)]["Volume"] += entry["Volume"]
            aggregated_data[str(week_start)]["Abandons"] += entry["Abandons"]
            aggregated_data[str(week_start)]["Top Line Agents (FTE)"] += entry[
                "Top Line Agents (FTE)"
            ]
            aggregated_data[str(week_start)]["Base AHT Sum"] += entry["Base AHT"]
            aggregated_data[str(week_start)]["Handled Threshold"] += entry[
                "Handled Threshold"
            ]
            aggregated_data[str(week_start)]["Service Level Sum"] += entry[
                "Service Level (X Seconds)"
            ]
            aggregated_data[str(week_start)]["Acceptable Wait Time"] += entry[
                "Acceptable Wait Time"
            ]
            aggregated_data[str(week_start)]["Total Queue Time"] += entry[
                "Total Queue Time"
            ]

        result = []
        for week_start, values in aggregated_data.items():
            result.append(
                {
                    "Week": str(week_start),
                    "Volume": values["Volume"],
                    "Abandons": values["Abandons"],
                    "Top Line Agents (FTE)": values["Top Line Agents (FTE)"],
                    "Base AHT": values["Base AHT Sum"] / len_of_value,
                    "Handled Threshold": values["Handled Threshold"],
                    "Service Level (X Seconds)": values["Service Level Sum"]
                    / len_of_value,
                    "Acceptable Wait Time": values["Acceptable Wait Time"]
                    / len_of_value,
                    "Total Queue Time": values["Total Queue Time"],
                }
            )

        result_data[key] = result

    return result_data


def handle_message(data):
    """
    Simulate a long-running task and send the processed result to Kafka.
    """
    print(f"Processing message: {data}")
    try:
        time.sleep(5)
        aggregated_data = extract_weekly_data(data["weekly"])

        response_payload = {
            "event": "rollup",
            "organizationId": data["organizationId"],
            "week_start": data["week_start"],
            "weekly": aggregated_data,
        }

        produce_response_message(KAFKA_RESPONSE_TOPIC, response_payload)

        print(f"Finished processing message")

    except Exception as e:
        print(f"Error processing message: {data}. Error: {e}")


def receive_messages():
    """
    Consume messages from the Kafka topic and process them concurrently.
    """
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset="earliest",
        value_deserializer=decode_message,
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
