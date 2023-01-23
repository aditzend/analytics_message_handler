import logging
import os
import pika
import logging
import requests
import json
import coloredlogs
from dotenv import load_dotenv
from pathlib import Path
import redis

load_dotenv()

FORMAT = (
    "[MESSAGE_HANDLER] %(process)d  - %(asctime)s     %(levelname)s"
    " [%(module)s] %(message)s"
)
logging.basicConfig(level=logging.INFO, format=FORMAT)
coloredlogs.install(level="INFO", fmt=FORMAT)
logger = logging.getLogger(__name__)


redis_host = os.getenv("REDIS_HOST")
redis_port = os.getenv("REDIS_PORT")
redis_pool = redis.ConnectionPool(host=redis_host, port=redis_port, db=0)
r = redis.Redis(connection_pool=redis_pool)

# processed_path = os.getenv("PROCESSED_PATH")
# error_path = os.getenv("ERROR_PATH")
# basepath = Path(os.getenv("CALL_RECORDINGS_PATH"))
# split_path = os.getenv("SPLIT_PATH")
rabbitmq_host = os.getenv("RABBITMQ_HOST") or "192.168.43.169"
rabbitmq_port = os.getenv("RABBITMQ_PORT") or 30072
rabbitmq_exchange = os.getenv("RABBITMQ_EXCHANGE") or "analytics"
rabbitmq_queue = os.getenv("TRANSCRIPTION_FINISHED_QUEUE") or "random"


def mark_as_processed(ch, method, properties, body):
    # TODO: ingestor should be the only one to move files around
    # processed_path = os.getenv("PROCESSED_PATH")

    message = json.loads(body.decode())
    job = message["data"]
    logger.info(f"Marking job as processed: {job}")
    markTranscriptJobAsFinishedDto = {
        "interaction_id": job["interaction_id"],
        "id": job["id"],
        "utterances": job["utterances"],
    }
    updateUtterancesDto = {"utterances": job["utterances"]}
    logger.critical(f"update dto {updateUtterancesDto}")
    try:
        # TODO: finish after nlp is ready
        requests.post(
            f"{os.getenv('ANALYTICS_MANAGER_URL')}/v3/jobs/finished",
            json=markTranscriptJobAsFinishedDto,
        )
        utterancesUpdate = requests.put(
            f"{os.getenv('ANALYTICS_MANAGER_URL')}/v3/interaction/{job['interaction_id']}/utterances",
            json=updateUtterancesDto,
        )

        if utterancesUpdate.status_code != 200:
            raise Exception(
                f"Error updating utterances: {utterancesUpdate.status_code}"
            )
        # Move status in redis, ingestor needs to know
        r.set(job["interaction_id"], "TRANSCRIPTION_FINISHED")

        # ACK de the message in the transcription queue
        ch.basic_ack(delivery_tag=method.delivery_tag)
    # TODO: ingestor should be the only one to move files around

    # shutil.move(job["audio_url"], processed_path)
    except Exception as error:
        logging.error(f"{error}")


try:
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(rabbitmq_host, rabbitmq_port),
    )
    channel = connection.channel()
    channel.exchange_declare(
        rabbitmq_exchange, exchange_type="topic", durable=True
    )
    result = channel.queue_declare(
        rabbitmq_queue, exclusive=False, durable=True
    )
    queue_name = result.method.queue
    channel.queue_bind(
        exchange=rabbitmq_exchange,
        queue=queue_name,
        routing_key="transcription.*.finished.*",
    )
    logger.info(f"Waiting for finishedtranscription jobs on {queue_name}")
    channel.basic_consume(
        queue=queue_name, on_message_callback=mark_as_processed, auto_ack=False
    )
    channel.start_consuming()
except Exception as error:
    logger.error(f"Error: {error}")
