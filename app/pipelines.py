import logging
import intent_processor
import nlp_processor
from types import SimpleNamespace
import json
import pandas as pd


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
    events: list
    entities: list
    sentiments: list
    intent_groups: list
    intent_subgroups: list
    emotions: list
    hate_speech_flag: bool
    neg_sentiment_flag: bool
    anger_emotion_flag: bool
    disgust_emotion_flag: bool


logger = logging.getLogger(__name__)


def process(channel, method, properties, body):
    try:
        job = json.loads(body.decode())
        job = job["data"]
        j = SimpleNamespace(**job)
        pipeline = j.pipeline or "default"
        utterances = j.utterances

        logger.debug(f"utterances {utterances[0]} type {type(utterances[0])}")
        # TODO: For now, segment id is the same as interaction id with id 1

        # TODO: Default is the only pipeline for now
        if pipeline == "default":
            events = []
            entities = []
            sentiments = []
            intent_groups = []
            intent_subgroups = []
            emotions = []
            hate_speech_flag = False
            neg_sentiment_flag = False
            anger_emotion_flag = False
            disgust_emotion_flag = False
            event_position = 1
            for utterance in utterances:
                utterance = SimpleNamespace(**utterance)
                event_id = j.interaction_id + "-s1-e" + str(event_position)
                intent = intent_processor.parse(
                    interaction_id=j.interaction_id,
                    text=utterance.text,
                    channel=utterance.channel,
                )

                nlp = nlp_processor.parse(
                    interaction_id=j.interaction_id,
                    text=utterance.text,
                    channel=utterance.channel,
                )

                event = {
                    "interaction_id": j.interaction_id,
                    "segment_id": j.interaction_id + "-s1",
                    "id": event_id,
                    "type": "utterance",
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
                        entities.append(
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
                        entity = SimpleNamespace(**entity)
                        entities.append(
                            {
                                "score": entity.score,
                                "entity": entity.type,
                                "value": entity.text,
                                "event_id": event_id,
                                "start": entity.start,
                                "end": entity.end,
                                "channel": utterance.channel,
                            }
                        )
                intent_groups.append(intent["intent_level_1"])
                intent_subgroups.append(intent["intent_level_2"])
                emotions.append(nlp["emotion"]["output"])
                sentiments.append(nlp["sentiment"]["output"])
                if nlp["hate_speech"]["output"]:
                    hate_speech_flag = True

                if nlp["sentiment"]["output"] == "NEG":
                    neg_sentiment_flag = True

                if nlp["emotion"]["output"] == "anger":
                    anger_emotion_flag = True

                if nlp["emotion"]["output"] == "disgust":
                    disgust_emotion_flag = True

                events.append(event)
                event_position += 1

            # logger.critical(f"events {events}")
            # logger.critical(f"sentiments {sentiments}")
            # logger.critical(f"hate_speech_flag {hate_speech_flag}")
            # logger.critical(f"neg_sentiment_flag {neg_sentiment_flag}")
            # logger.critical(f"anger_emotion_flag {anger_emotion_flag}")
            # logger.critical(f"disgust_emotion_flag {disgust_emotion_flag}")

            # use pd to get the most common sentiment
            main_sentiment = pd.Series(sentiments).value_counts().index[0]
            # logger.critical(f"main_sentiment {main_sentiment}")

            # use pd to get the most common emotion
            main_emotion = pd.Series(emotions).value_counts().index[0]
            # logger.critical(f"main_emotion {main_emotion}")

            # use pd to get the most common intent group
            # and filter out the "Base" intent
            main_intent_group = (
                pd.Series(intent_groups).value_counts().drop("Base").index[0]
            )

            # logger.critical(f"main_intent_group {main_intent_group}")

            # use pd to get the most common intent subgroup
            main_intent_subgroup = (
                pd.Series(intent_subgroups).value_counts().index[0]
            )
            # logger.critical(f"main_intent_subgroup {main_intent_subgroup}")

            # logger.critical(f"entities {entities}")

    except AttributeError as error:
        logging.error(f"{error}")
        raise
    except ValueError as error:
        logging.error(f"{error}")
        raise
