import os
import requests
import logging
from types import SimpleNamespace
from pydantic import BaseModel

logger = logging.getLogger(__name__)


rasa_host = os.getenv("RASA_HOST") or "192.168.43.170"
rasa_port = os.getenv("RASA_PORT") or "30000"
rasa_url = f"http://{rasa_host}:{rasa_port}"
webhook_url = f"{rasa_url}/webhooks/rest/webhook"
model_parse_url = f"{rasa_url}/model/parse"
status_url = f"{rasa_url}/status"

logger.info(f"Rasa webhook url: {webhook_url}")


class Intent(BaseModel):
    model_version: str
    model_name: str
    intent_name: str
    intent_confidence: float
    intent_level_1: str
    intent_level_2: str or None
    intent_level_3: str or None
    intent_level_4: str or None
    entities: list or None
    group: str or None
    labels: list or None


def parse(interaction_id, text, channel):
    logger.debug(f"Starting intent parsing for {interaction_id}")
    logger.debug(f"Rasa webhook url: {webhook_url}")
    logger.debug(f"Rasa model parse url: {model_parse_url}")

    try:
        model_data = requests.get(status_url)
        model_data = model_data.json()
        model_data = SimpleNamespace(**model_data)

        intent_data = requests.post(
            model_parse_url,
            json={"message_id": interaction_id, "text": text, "lang": "es"},
        )

        intent_data = intent_data.json()
        intent_data = SimpleNamespace(**intent_data)

        intent_levels = intent_data.intent["name"].split("_")
        level_count = len(intent_levels)

        conversation_response = requests.post(
            webhook_url,
            json={
                "sender": interaction_id,
                "message": text,
                "channel": channel,
            },
        )

        conversation_response = conversation_response.json()[0]
        # Siempre primero poner la respuesta y despues la accion.

        # conversation_response = SimpleNamespace(**conversation_response)
        group = ""
        labels = ""
        if "group" in conversation_response.keys():
            # logger.info(f"Rasa response: {conversation_response.group}")
            group = conversation_response["group"]

        if "labels" in conversation_response.keys():
            # logger.info(f"Rasa response: {conversation_response.labels}")
            labels = conversation_response["labels"]
        conversation_response = SimpleNamespace(**conversation_response)

        response = {
            "model_version": model_data.fingerprint["trained_at"],
            "model_name": model_data.model_file,
            "intent_name": intent_data.intent["name"],
            "intent_confidence": intent_data.intent["confidence"],
            "intent_level_1": intent_levels[level_count - 1],
            "intent_level_2": intent_levels[level_count - 2]
            if level_count > 1
            else "",
            "intent_level_3": intent_levels[level_count - 3]
            if level_count > 2
            else "",
            "intent_level_4": intent_levels[level_count - 4]
            if level_count > 3
            else "",
            "entities": intent_data.entities,
            "group": group,
            "labels": labels,
        }
        logger.debug(f"Intent response: {response}")
        return response
    except IOError as error:
        logger.error(f"IO Error: {error}")
