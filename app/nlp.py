import logging
import json
import intents
import sentiment

logger = logging.getLogger(__name__)


def do_nlp(ch, method, properties, body):
    # processed_path = os.getenv("PROCESSED_PATH")

    message = json.loads(body.decode())
    job = message["data"]
    # logger.info(f"Marking job as processed: {job}")
    # markTranscriptJobAsFinishedDto = {
    #     "interaction_id": job["interaction_id"],
    #     "id": job["id"],
    #     "utterances": job["utterances"],
    # }
    updateUtterancesDto = {"utterances": job["utterances"]}
    logger.debug(f"update dto {updateUtterancesDto}")
    try:
        # requests.post(
        #     f"{os.getenv('ANALYTICS_MANAGER_URL')}/v3/jobs/finished",
        #     json=markTranscriptJobAsFinishedDto,
        # )
        # utterancesUpdate = requests.put(
        #     f"{os.getenv('ANALYTICS_MANAGER_URL')}/v3/interaction/{job['interaction_id']}/utterances",
        #     json=updateUtterancesDto,
        # )
        for utterance in job["utterances"]:
            intents.parse(
                interaction_id=job["interaction_id"],
                text=utterance["text"],
                channel=utterance["channel"],
            )
            sentiment.parse(
                interaction_id=job["interaction_id"],
                text=utterance["text"],
                channel=utterance["channel"],
            )

        # if utterancesUpdate.status_code != 200:
        #     raise Exception(
        #         f"Error updating utterances: {utterancesUpdate.status_code}"
        #     )
        # # Move status in redis, ingestor needs to know
        # r.set(job["interaction_id"], "TRANSCRIPTION_FINISHED")

        # ACK de the message in the transcription queue
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as error:
        logging.error(f"{error}")
