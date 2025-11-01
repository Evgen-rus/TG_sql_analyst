import os
import logging
from dotenv import load_dotenv

# Загружаем .env из корня
load_dotenv()

# OpenAI
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = "gpt-5-mini"  # по требованию
TRANSCRIPTION_MODEL = "gpt-4o-mini-transcribe"  # по требованию

# Параметры для Responses API (маппинг max_tokens обработаем в агентах)
OPENAI_PARAMS = {
    "max_tokens": 5000,
}

# Telegram
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")

# Разрешённые чаты: берём из ALLOWED_CHAT_IDS или TELEGRAM_CHAT_ID (через запятую)
_allowed_from_env = os.getenv("ALLOWED_CHAT_IDS") or os.getenv("TELEGRAM_CHAT_ID", "")
ALLOWED_CHAT_IDS = []
if _allowed_from_env:
    # поддержим как числа, так и строки
    ALLOWED_CHAT_IDS = [item.strip() for item in _allowed_from_env.split(",") if item.strip()]

# Путь к БД (по умолчанию leads.db в корне проекта)
DB_PATH = os.getenv("DB_PATH", os.path.join(os.getcwd(), "leads.db"))

# Логирование только в консоль (по требованию)
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logger = logging.getLogger("tg_sql_analyst")
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(handler)
logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))