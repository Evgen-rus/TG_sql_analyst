from typing import Dict, Any, List
import json
from openai import AsyncOpenAI

from config import OPENAI_API_KEY, OPENAI_MODEL, OPENAI_PARAMS, logger
from prompts import ANALYST_SYSTEM_PROMPT
from project_resolver import get_code_to_tag_map
import re


client = AsyncOpenAI(api_key=OPENAI_API_KEY)


def _build_kwargs(input_text: str) -> Dict[str, Any]:
    kwargs: Dict[str, Any] = {
        "model": OPENAI_MODEL,
        "input": input_text,
        "instructions": ANALYST_SYSTEM_PROMPT,
    }
    max_tokens = OPENAI_PARAMS.get("max_tokens")
    if isinstance(max_tokens, int) and max_tokens > 0:
        kwargs["max_output_tokens"] = max_tokens
    return kwargs


def _safe_json_loads(text: str) -> Dict[str, Any]:
    try:
        return json.loads(text)
    except Exception:
        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end != -1 and end > start:
            try:
                return json.loads(text[start:end+1])
            except Exception:
                pass
    return {}


def _sanitize_payload_for_log(payload: Dict[str, Any]) -> str:
    """Возвращает безопасный для логов JSON payload: маскирует возможные PII (например, телефоны)."""
    try:
        # Глубокая копия через JSON сериализацию, чтобы не мутировать исходные объекты
        data = json.loads(json.dumps(payload, ensure_ascii=False))
        result = data.get("result")
        if isinstance(result, list):
            for row in result:
                if isinstance(row, dict) and "phone" in row:
                    raw = str(row.get("phone", ""))
                    # Простая маскировка: оставим первые 2 и последние 2 символа, остальное заменим
                    if len(raw) >= 4:
                        row["phone"] = raw[:2] + "***" + raw[-2:]
                    elif raw:
                        row["phone"] = "***"
        # Ограничим длину, чтобы не засорять логи
        return json.dumps(data, ensure_ascii=False)[:2000]
    except Exception:
        try:
            return str(payload)[:2000]
        except Exception:
            return "<payload>"


def _format_input(user_question: str, sql: str, result_rows: List[Dict[str, Any]], code_names: Dict[str, str]) -> str:
    payload = {
        "user_question": user_question,
        "sql": sql,
        "result": result_rows,
        "project_names": code_names,  # { '[LR166]': '[LR166] ПромСпецАвто Татьяна' }
        "note": "Отвечай на языке вопроса. В ответе показывай человеко-понятные названия проектов (project_names), а не только коды.",
    }
    # Логируем безопасный превью payload (с маскировкой PII)
    try:
        logger.info("Analyst payload preview (sanitized): %s", _sanitize_payload_for_log(payload))
    except Exception:
        pass
    return json.dumps(payload, ensure_ascii=False)


async def generate_answer(user_question: str, sql: str, result_rows: List[Dict[str, Any]]) -> Dict[str, str]:
    # Построим соответствия кодов -> имена из глобального маппинга, по кодам найденным в result или в тексте SQL
    code2name: Dict[str, str] = {}
    try:
        reverse_map = get_code_to_tag_map()  # 'LR166' -> '[LR166] ПромСпецАвто Татьяна'
        seen: set[str] = set()
        # 1) из result_rows
        for row in result_rows or []:
            code = str(row.get("project_code") or "").strip()
            if not code:
                continue
            # Не снимаем скобки: используем код как есть, например "[LR165]"
            code_clean = code
            if code in seen:
                continue
            seen.add(code)
            name_full = reverse_map.get(code_clean, "")
            if name_full:
                code2name[code] = name_full
        # 2) как запасной вариант — вытащим [LRxxx] из SQL
        if not code2name and sql:
            for m in re.findall(r"\[[A-Za-z]{2}\d+\]", sql):
                code = m
                # Не снимаем скобки
                code_clean = code
                if code in seen:
                    continue
                seen.add(code)
                name_full = reverse_map.get(code_clean, "")
                if name_full:
                    code2name[code] = name_full
    except Exception:
        pass

    try:
        logger.info(
            "Analyst input: rows=%s, project_names=%s, question_len=%s",
            len(result_rows or []), list(code2name.keys()) if code2name else [], len(user_question or "")
        )
    except Exception:
        pass

    input_text = _format_input(user_question or "", sql or "", result_rows or [], code2name)
    try:
        # Короткий лог перед отправкой в модель, без раскрытия содержимого
        try:
            logger.info("Analyst: sending payload to model (chars=%s)", len(input_text or ""))
        except Exception:
            pass
        kwargs = _build_kwargs(input_text)
        resp = await client.responses.create(**kwargs)
        text = getattr(resp, "output_text", "") or ""
        if text:
            logger.info("Analyst raw output_text: %s", text[:400].replace("\n", " "))
        data = _safe_json_loads(text)
        answer = str(data.get("answer") or "").strip()
        analysis = str(data.get("analysis") or "").strip()
        if not answer:
            answer = "Не удалось сформировать ответ"
        return {"answer": answer, "analysis": analysis}
    except Exception as exc:
        logger.error("Аналитик-агент: ошибка Responses API: %s", exc)
        return {"answer": "Произошла ошибка при формировании ответа.", "analysis": ""}


