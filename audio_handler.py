from typing import Optional
from openai import AsyncOpenAI

from config import OPENAI_API_KEY, TRANSCRIPTION_MODEL, logger


client = AsyncOpenAI(api_key=OPENAI_API_KEY)


async def transcribe_voice(voice_data: bytes, file_name: str = "voice.ogg", language: Optional[str] = None) -> str:
    try:
        logger.info("Транскрибация голосового сообщения (%s байт)", len(voice_data))
        kwargs = {
            "model": TRANSCRIPTION_MODEL,
            "file": (file_name, voice_data),
        }
        if language:
            kwargs["language"] = language
        transcript = await client.audio.transcriptions.create(**kwargs)
        text = transcript.text or ""
        return text.strip()
    except Exception as exc:
        logger.error("Ошибка транскрибации: %s", exc)
        raise


