import logging
import os

THRESHOLD = int(os.getenv("QUEUE_THRESHOLD", "500000"))
QUEUE_PATTERN = os.getenv("QUEUE_PATTERN", "task")
CONTAINER_NAME = os.getenv("RABBITMQ_CONTAINER", "rabbitmq")
DAILY_INFO_FILE = os.getenv("DAILY_INFO_FILE", "./data/daily_info.txt")
DOCKER_LOG_HOURS = int(os.getenv("DOCKER_LOG_HOURS", "24"))
DOCKER_LOG_MAX_CHARS = int(os.getenv("DOCKER_LOG_MAX_CHARS", "12000"))

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-5.6-luna")
OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL", "").strip()

WECOM_WEBHOOK = os.getenv("WECOM_WEBHOOK", "")
WECOM_MAX_ATTEMPTS = 3

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("rabbitmq-inspection-agent")
