import pika
import logging
import os
from dotenv import load_dotenv
import json

load_dotenv()


class RabbitMQ:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.host = os.getenv("RABBITMQ_HOST") or ""
        self.port = os.getenv("RABBITMQ_PORT") or 0
        self.transcript_exchange = (
            os.getenv("RABBITMQ_TRANSCRIPT_EXCHANGE") or ""
        )
        self.all_finished_exchange = (
            os.getenv("RABBITMQ_ALL_FINISHED_EXCHANGE") or ""
        )
        self.transcript_queue = os.getenv("TRANSCRIPTION_FINISHED_QUEUE") or ""
        self.all_finished_queue = os.getenv("ALL_FINISHED_QUEUE") or ""
        self.consume_connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=self.host,
                port=self.port,
                client_properties={
                    "connection_name": "transcription-finished-connection"
                },
            )
        )
        self.publish_connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=self.host,
                port=self.port,
                client_properties={
                    "connection_name": "all-finished-connection"
                },
            )
        )

    def consume(self, callback):
        try:
            channel = self.consume_connection.channel()
            channel.exchange_declare(
                self.transcript_exchange,
                exchange_type="topic",
                durable=True,
            )
            result = channel.queue_declare(
                self.transcript_queue, exclusive=False, durable=True
            )
            queue_name = result.method.queue
            channel.queue_bind(
                exchange=self.transcript_exchange,
                queue=queue_name,
                routing_key="transcription.*.finished.*",
            )
            self.logger.info(
                f"Waiting for finished transcription jobs on {queue_name}"
            )

            channel.basic_consume(
                queue=queue_name,
                on_message_callback=callback,
                auto_ack=False,
            )
            channel.start_consuming()
        except IOError as error:
            self.logger.error(f"Error: {error}")

    def publish(self, message):
        try:
            channel = self.publish_connection.channel()
            # data = json.dumps(message)
            payload = {
                "pattern": {
                    "group": "analytics",
                    "process": "nlp",
                },
                "data": message[0],
            }
            payload = json.dumps(payload)
            payload = str(payload)

            channel.exchange_declare(
                exchange=self.all_finished_exchange,
                durable=True,
                exchange_type="fanout",
            )
            result = channel.queue_declare(
                queue=self.all_finished_queue, exclusive=False, durable=True
            )

            queue_name = result.method.queue

            channel.queue_bind(
                exchange=self.all_finished_exchange,
                queue=queue_name,
                routing_key="all.*.finished.*",
            )

            channel.basic_publish(
                exchange=self.all_finished_exchange,
                routing_key="all.*.finished.*",
                body=payload,
            )
            self.logger.info(f"Published message: {payload}")
            self.publish_connection.close()
        except IOError as error:
            self.logger.error(f"Error: {error}")
            raise
