import asyncio
import json
import time
from typing import Any, Dict, List, Optional

import requests

from config import TELEGRAM_BOT_TOKEN, ALLOWED_CHAT_IDS, logger
from openai_sql_agent import generate_sql
from openai_analyst_agent import generate_answer
from audio_handler import transcribe_voice
from db import execute_select


API_BASE = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"
FILE_BASE = f"https://api.telegram.org/file/bot{TELEGRAM_BOT_TOKEN}"


def _is_allowed_chat(chat_id: Any) -> bool:
    if not ALLOWED_CHAT_IDS:
        return True  # если список пуст — разрешаем всё
    return str(chat_id) in set(str(x) for x in ALLOWED_CHAT_IDS)


def tg_get_updates(offset: Optional[int] = None, timeout: int = 50) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {"timeout": timeout}
    if offset is not None:
        params["offset"] = offset
    params["allowed_updates"] = ["message", "channel_post"]
    resp = requests.get(f"{API_BASE}/getUpdates", params=params, timeout=timeout + 5)
    resp.raise_for_status()
    body = resp.json()
    if not body.get("ok"):
        raise RuntimeError(f"Telegram API error: {body}")
    return body.get("result", [])


def tg_send_message(chat_id: Any, text: str) -> None:
    payload = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": True,
    }
    resp = requests.post(f"{API_BASE}/sendMessage", json=payload, timeout=15)
    resp.raise_for_status()


def tg_get_file(file_id: str) -> str:
    resp = requests.get(f"{API_BASE}/getFile", params={"file_id": file_id}, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    if not data.get("ok"):
        raise RuntimeError(f"getFile error: {data}")
    return data["result"]["file_path"]


def tg_download_file(file_path: str) -> bytes:
    url = f"{FILE_BASE}/{file_path}"
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    return resp.content


def _extract_text_and_voice(update: Dict[str, Any]) -> Dict[str, Any]:
    container = update.get("message") or update.get("channel_post") or {}
    chat = (container.get("chat") or {})
    chat_id = chat.get("id")
    text = container.get("text")
    voice = container.get("voice")
    return {"chat_id": chat_id, "text": text, "voice": voice}


def _format_final_text(answer: str, analysis: str) -> str:
    if analysis:
        return f"{answer}\n\n{analysis}"
    return answer


async def _text_flow_async(text: str) -> str:
    sql_obj = await generate_sql(text)
    sql = sql_obj.get("sql") or ""
    explanation = sql_obj.get("explanation") or ""
    if not sql:
        return explanation or "Не удалось сгенерировать SQL."

    try:
        rows = execute_select(sql)
    except Exception as exc:
        return f"Ошибка выполнения SQL: {exc}"

    analyst = await generate_answer(text, sql, rows)
    return _format_final_text(analyst.get("answer", ""), analyst.get("analysis", ""))


def handle_text_flow_sync(text: str) -> str:
    return asyncio.run(_text_flow_async(text))


def handle_voice_flow_sync(voice: Dict[str, Any]) -> str:
    file_id = voice.get("file_id")
    if not file_id:
        return "Голосовой файл не найден"

    # качаем файл и транскрибируем
    file_path = tg_get_file(file_id)
    content = tg_download_file(file_path)

    async def _run() -> str:
        text = await transcribe_voice(content, file_name="voice.ogg", language=None)
        if not text:
            return "Не удалось распознать голосовое сообщение"
        return await _text_flow_async(text)

    return asyncio.run(_run())


def main() -> None:
    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN не задан в .env")

    logger.info("Старт поллинга Telegram")
    offset: Optional[int] = None
    try:
        while True:
            try:
                updates = tg_get_updates(offset=offset, timeout=50)
            except Exception as e:
                logger.error("getUpdates error: %s", e)
                time.sleep(2)
                continue

            if not updates:
                continue

            for upd in updates:
                offset = upd["update_id"] + 1
                data = _extract_text_and_voice(upd)
                chat_id = data.get("chat_id")
                if chat_id is None:
                    continue
                if not _is_allowed_chat(chat_id):
                    logger.info("Сообщение из неразрешённого чата: %s", chat_id)
                    continue

                text = data.get("text")
                voice = data.get("voice")

                try:
                    if text:
                        try:
                            logger.info("Incoming text len=%s from chat=%s", len(text or ""), chat_id)
                        except Exception:
                            pass
                        reply = handle_text_flow_sync(text)
                    elif voice:
                        logger.info("Incoming voice from chat=%s", chat_id)
                        reply = handle_voice_flow_sync(voice)
                    else:
                        reply = "Поддерживаются текст и голосовые сообщения."
                except Exception as exc:
                    logger.error("Обработка сообщения завершилась ошибкой: %s", exc)
                    reply = "Произошла ошибка при обработке сообщения."

                try:
                    try:
                        logger.info("Reply len=%s to chat=%s", len(reply or ""), chat_id)
                    except Exception:
                        pass
                    tg_send_message(chat_id, reply)
                except Exception as exc:
                    logger.error("sendMessage error: %s", exc)

            time.sleep(0.3)
    except KeyboardInterrupt:
        logger.info("Остановлено пользователем")


if __name__ == "__main__":
    main()


