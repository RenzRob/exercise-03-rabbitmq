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
import sys
import time

import pika


def get_rabbit_url() -> str:
    return os.environ.get("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")


def main():
    url = get_rabbit_url()
    params = pika.URLParameters(url)
    while True:
        try:
            conn = pika.BlockingConnection(params)
            channel = conn.channel()
            channel.queue_declare(queue="node_events", durable=True)

            def callback(ch, method, properties, body):
                try:
                    payload = json.loads(body)
                    event = payload.get("event")
                    node_name = payload.get("node_name")
                    timestamp = payload.get("timestamp")
                    print(f"EVENT: {event} | node: {node_name} | time: {timestamp}")
                    sys.stdout.flush()
                except Exception:
                    print("EVENT: invalid_message | node: unknown | time: unknown")
                    sys.stdout.flush()
                ch.basic_ack(delivery_tag=method.delivery_tag)

            channel.basic_consume(queue="node_events", on_message_callback=callback)
            print("Consumer started, waiting for messages...")
            sys.stdout.flush()
            channel.start_consuming()

        except KeyboardInterrupt:
            try:
                conn.close()
            except Exception:
                pass
            print("Consumer stopped by user")
            break
        except Exception:
            # Retry connection after a short delay
            try:
                conn.close()
            except Exception:
                pass
            time.sleep(2)


if __name__ == "__main__":
    main()
