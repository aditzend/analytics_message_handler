import logging
import coloredlogs
from dotenv import load_dotenv
from rabbit import RabbitMQ
import pipelines

load_dotenv()

FORMAT = (
    "[NLP] - %(asctime)s %(levelname)7s %(module)-20s "
    "%(threadName)-10s %(message)s "
)
logging.basicConfig(level=logging.INFO, format=FORMAT)
coloredlogs.install(level="INFO", fmt=FORMAT)
logger = logging.getLogger(__name__)


# Listen to exchange on a parallel thread

# fq.consume()
# rabbitmq_thread = threading.Thread(target=fq.consume())
# rabbitmq_thread.start()


rabbit = RabbitMQ()
rabbit.consume(pipelines.process)
