import logging
import os
import pika
import coloredlogs
from dotenv import load_dotenv
from pipelines import process


load_dotenv()

FORMAT = (
    "[MESSAGE_HANDLER] %(process)d  - %(asctime)s     %(levelname)s"
    " [%(module)s] %(message)s"
)
logging.basicConfig(level=logging.INFO, format=FORMAT)
coloredlogs.install(level="INFO", fmt=FORMAT)
logger = logging.getLogger(__name__)

rabbitmq_host = os.getenv("RABBITMQ_HOST") or ""
rabbitmq_port = os.getenv("RABBITMQ_PORT") or 0
rabbitmq_transcript_exchange = os.getenv("RABBITMQ_TRANSCRIPT_EXCHANGE") or ""
rabbitmq_queue = os.getenv("TRANSCRIPTION_FINISHED_QUEUE") or ""


try:
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(rabbitmq_host, rabbitmq_port),
    )
    channel = connection.channel()
    channel.exchange_declare(
        rabbitmq_transcript_exchange, exchange_type="topic", durable=True
    )
    result = channel.queue_declare(
        rabbitmq_queue, exclusive=False, durable=True
    )
    queue_name = result.method.queue
    channel.queue_bind(
        exchange=rabbitmq_transcript_exchange,
        queue=queue_name,
        routing_key="transcription.*.finished.*",
    )
    logger.info(f"Waiting for finished transcription jobs on {queue_name}")

    channel.basic_consume(
        queue=queue_name, on_message_callback=process, auto_ack=False
    )
    channel.start_consuming()
except IOError as error:
    logger.error(f"Error: {error}")
