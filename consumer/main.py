"""Exercise 03 — Event Consumer

Connects to RabbitMQ (RABBITMQ_URL) and consumes messages from the
`node_events` queue. Each message is expected to be JSON with keys:
`event`, `node_name`, `timestamp`.

Logs to stdout in the exact format:
  EVENT: {event} | node: {node_name} | time: {timestamp}

Acknowledges each message after processing.
"""

import json
import os
import time

import pika

RABBITMQ_URL = os.environ.get("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
QUEUE_NAME = "node_events"


def callback(ch, method, properties, body):
    try:
        payload = json.loads(body)
        event = payload.get("event", "unknown")
        node_name = payload.get("node_name", "unknown")
        timestamp = payload.get("timestamp", "unknown")
        print(f"EVENT: {event} | node: {node_name} | time: {timestamp}", flush=True)
    except Exception as e:
        print(f"[ERROR] Failed to process message: {e}", flush=True)
    ch.basic_ack(delivery_tag=method.delivery_tag)


def connect_with_retry(url: str, retries: int = 10, delay: int = 3):
    for attempt in range(1, retries + 1):
        try:
            params = pika.URLParameters(url)
            connection = pika.BlockingConnection(params)
            print("[INFO] Connected to RabbitMQ", flush=True)
            return connection
        except Exception as e:
            print(f"[WARN] Attempt {attempt}/{retries} failed: {e}", flush=True)
            time.sleep(delay)
    raise RuntimeError("Could not connect to RabbitMQ after multiple attempts")


def main():
    connection = connect_with_retry(RABBITMQ_URL)
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback)
    print(f"[INFO] Waiting for messages on queue '{QUEUE_NAME}'...", flush=True)
    channel.start_consuming()


if __name__ == "__main__":
    main()
