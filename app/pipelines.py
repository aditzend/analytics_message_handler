import logging
import intent_processor
import nlp_processor
from types import SimpleNamespace
import json
import pandas as pd
from rabbit import RabbitMQ
import datetime

class Utterance(object):
    text: str
    channel: float
    start: float
    end: float


class Entity(object):
    score: float
    entity: str
    value: str
    event_id: str
    start: float
    end: float
    channel: float


class ProcessResult(object):
    def __init__(self):
        self.events = []
        self.entities = []
        self.sentiments = []
        self.intent_groups = []
        self.intent_subgroups = []
        self.emotions = []
        self.main_emotion = ""
        self.main_intent_group = ""
        self.main_intent_subgroup = ""
        self.main_sentiment = ""
        self.hate_speech_flag = False
        self.neg_sentiment_flag = False
        self.joy_emotion_flag = False
        self.sadness_emotion_flag = False
        self.surprise_emotion_flag = False
        self.fear_emotion_flag = False
        self.anger_emotion_flag = False
        self.disgust_emotion_flag = False


logger = logging.getLogger(__name__)


def process(ch, method, properties, body):
    result = ProcessResult()
    rabbit = RabbitMQ()
    try:
        job = json.loads(body.decode())
        j = SimpleNamespace(**job["data"])
        pipeline = j.pipeline or "default"
        utterances = j.utterances

        # TODO: For now, segment id is the same as interaction id with id 1

        # TODO: Default is the only pipeline for now
        if pipeline == "default":
            event_position = 1
            # format date as YYYY-MM-DD HH:MM:SS
            datetime.datetime.now().strftime("-D%Y-%m-%dT%H:%M")
            current_minute = datetime.datetime.now().strftime("-%Y-%m-%dT%H:%M")
            sender_id = j.interaction_id + str(current_minute)
            for utterance in utterances:
                utterance = SimpleNamespace(**utterance)
                event_id = j.interaction_id + "-s1-e" + str(event_position)


                nlp = nlp_processor.parse(
                    interaction_id=sender_id,
                    text=utterance.text,
                    channel=utterance.channel,
                )

                message_for_rasa = f'{utterance.text} [{nlp["emotion"]["output"].upper()}] [{nlp["sentiment"]["output"]}]'
                intent = intent_processor.parse(
                    interaction_id=sender_id,
                    text=message_for_rasa,
                    channel=utterance.channel,
                )                

                event = {
                    "text": utterance.text,
                    "interaction_id": j.interaction_id,
                    "segment_id": j.interaction_id + "-s1",
                    "event_id": event_id,
                    "type": "utterance",
                    "start": utterance.start,
                    "end": utterance.end,
                    "channel": utterance.channel,
                    "nlp": {
                        "intent": intent,
                        "entities": intent["entities"],
                        "sentiment": nlp["sentiment"],
                        "emotion": nlp["emotion"],
                        "ner": nlp["ner"],
                        "pos": nlp["pos"],
                        "hate_speech": nlp["hate_speech"],
                    },
                }

                if intent["entities"]:
                    for entity in intent["entities"]:
                        entity = SimpleNamespace(**entity)
                        result.entities.append(
                            {
                                "score": entity.confidence_entity,
                                # entity must be all caps
                                "entity": entity.entity.upper(),
                                "value": entity.value,
                                "event_id": event_id,
                                "start": entity.start,
                                "end": entity.end,
                                "channel": utterance.channel,
                            }
                        )
                if nlp["ner"]:
                    for entity in nlp["ner"]:
                        if type(entity) is dict:
                            logger.warning(entity)
                            result.entities.append(
                                {
                                    "score": entity["score"] or 0.0,
                                    "entity": entity["type"] or "UNKNOWN",
                                    "value": entity["text"] or "",
                                    "event_id": event_id,
                                    "start": entity["start"] or 0.0,
                                    "end": entity["end"] or 0.0,
                                    "channel": utterance["channel"],
                                }
                            )
                result.intent_groups.append(intent["intent_level_1"])
                result.intent_subgroups.append(intent["intent_level_2"])
                result.emotions.append(nlp["emotion"]["output"])
                result.sentiments.append(nlp["sentiment"]["output"])

                if nlp["hate_speech"]["output"]:
                    result.hate_speech_flag = True

                if nlp["sentiment"]["output"] == "NEG":
                    result.neg_sentiment_flag = True

                if nlp["emotion"]["output"] == "joy":
                    result.joy_emotion_flag = True

                if nlp["emotion"]["output"] == "sadness":
                    result.sadness_emotion_flag = True

                if nlp["emotion"]["output"] == "surprise":
                    result.surprise_emotion_flag = True

                if nlp["emotion"]["output"] == "fear":
                    result.fear_emotion_flag = True

                if nlp["emotion"]["output"] == "anger":
                    result.anger_emotion_flag = True

                if nlp["emotion"]["output"] == "disgust":
                    result.disgust_emotion_flag = True

                result.events.append(event)
                event_position += 1

            # logger.critical(f"events {events}")
            # logger.critical(f"sentiments {sentiments}")
            # logger.critical(f"hate_speech_flag {hate_speech_flag}")
            # logger.critical(f"neg_sentiment_flag {neg_sentiment_flag}")
            # logger.critical(f"anger_emotion_flag {anger_emotion_flag}")
            # logger.critical(f"disgust_emotion_flag {disgust_emotion_flag}")

            # use pd to get the most common sentiment
            result.main_sentiment = str(
                (pd.Series(result.sentiments).value_counts().index[0])
            )
            # logger.critical(f"main_sentiment {main_sentiment}")

            # use pd to get the most common emotion
            result.main_emotion = str(
                (pd.Series(result.emotions).value_counts().index[0])
            )
            # logger.critical(f"main_emotion {main_emotion}")

            # use pd to get the most common intent group
            # and filter out the "Base" intent
            logger.error(result.intent_groups)
            main_intent_list = pd.Series(result.intent_groups).value_counts().drop("Base")

            if len(main_intent_list) > 0:
                result.main_intent_group = main_intent_list.index[0]
            else:
                result.main_intent_group = "Base"
            # logger.critical(f"main_intent_group {main_intent_group}")

            # use pd to get the most common intent subgroup
            result.main_intent_subgroup = str(
                (pd.Series(result.intent_subgroups).value_counts().index[0])
            )
            # logger.critical(f"main_intent_subgroup {main_intent_subgroup}")

            # logger.critical(f"entities {entities}")

        message = (
            {
                "interaction_id": j.interaction_id,
                "status": "ALL_FINISHED",
                "transcription": {
                    "transcription_job_id": j.transcription_job_id,
                    "base_path": j.base_path,
                    "audio_url": j.audio_url,
                    "asr_provider": j.asr_provider,
                    "asr_language": j.asr_language,
                    "sample_rate": j.sample_rate,
                    "num_samples": j.num_samples,
                    "channels": j.channels,
                    "audio_format": j.audio_format,
                    "is_silent": j.is_silent,
                    "utterances": j.utterances,
                },
                "events": result.events,
                "nlp": {
                    "pipeline": "default",
                    "main_sentiment": result.main_sentiment,
                    "main_emotion": result.main_emotion,
                    "main_intent_group": result.main_intent_group,
                    "main_intent_subgroup": result.main_intent_subgroup,
                    "hate_speech_flag": result.hate_speech_flag,
                    "neg_sentiment_flag": result.neg_sentiment_flag,
                    "joy_emotion_flag": result.joy_emotion_flag,
                    "sadness_emotion_flag": result.sadness_emotion_flag,
                    "surprise_emotion_flag": result.surprise_emotion_flag,
                    "fear_emotion_flag": result.fear_emotion_flag,
                    "anger_emotion_flag": result.anger_emotion_flag,
                    "disgust_emotion_flag": result.disgust_emotion_flag,
                    "entities": result.entities,
                },
            },
        )

        rabbit.publish(message)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except AttributeError as error:
        logging.error(f"{error}")
        raise
    except ValueError as error:
        logging.error(f"{error}")
        raise
