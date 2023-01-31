import os
import requests
import logging

logger = logging.getLogger(__name__)


nlp_host = os.getenv("NLP_HOST") or "192.168.43.170"
nlp_port = os.getenv("NLP_PORT") or "30080"
nlp_url = f"http://{nlp_host}:{nlp_port}"
full_url = f"{nlp_url}/full"
ner_url = f"{nlp_url}/ner"
pos_url = f"{nlp_url}/pos"


#  {'sentiment': {'output': 'NEU', 'probas': {'NEG': 0.022224154323339462, 'NEU': 0.8082956075668335, 'POS': 0.1694803088903427}}, 'ner': [], 'pos': [{'score': 0.8020748496055603, 'word': 'Hola', 'start': 0, 'end': 4, 'text': 'Hola', 'type': 'PROPN'}], 'emotion': {'probas': {'others': 0.714801549911499, 'joy': 0.14690600335597992, 'sadness': 0.04273635894060135, 'anger': 0.017662987112998962, 'surprise': 0.05193520709872246, 'disgust': 0.01325034536421299, 'fear': 0.012707558460533619}, 'output': 'others'}, 'hate_speech': {'probas': {'hateful': 0.10141389071941376, 'targeted': 0.03936700150370598, 'aggressive': 0.04830383509397507}, 'output': []}}


def parse(interaction_id, text, channel):
    logger.debug(f"Starting NLP parsing for {interaction_id}")

    try:
        nlp = {
            "sentiment": {},
            "ner": {},
            "pos": {},
            "emotion": {},
            "hate_speech": {},
        }
        full = requests.post(
            full_url,
            json={"text": text},
        )
        full = full.json()
        nlp["sentiment"]["output"] = full["sentiment"]["output"]
        nlp["sentiment"]["probas"] = full["sentiment"]["probas"]
        nlp["emotion"]["probas"] = full["emotion"]["probas"]
        nlp["emotion"]["output"] = full["emotion"]["output"]
        nlp["hate_speech"]["probas"] = full["hate_speech"]["probas"]
        nlp["hate_speech"]["output"] = full["hate_speech"]["output"]

        ner = requests.post(
            ner_url,
            json={"text": text},
        )
        ner = ner.json()
        nlp["ner"] = ner

        pos = requests.post(
            pos_url,
            json={"text": text},
        )
        pos = pos.json()
        nlp["pos"] = pos

        logger.debug(f"NLP response: {nlp}")
        return nlp
    except ValueError as error:
        logger.error(f"Error parsing text: {error}")
        raise
    except AttributeError as error:
        logger.error(f"Error parsing text: {error}")
        raise
