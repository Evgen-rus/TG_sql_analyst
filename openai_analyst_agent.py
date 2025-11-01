from typing import Dict, Any, List
import json
from openai import AsyncOpenAI

from config import OPENAI_API_KEY, OPENAI_MODEL, OPENAI_PARAMS, logger
from prompts import ANALYST_SYSTEM_PROMPT


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


def _format_input(user_question: str, sql: str, result_rows: List[Dict[str, Any]]) -> str:
    payload = {
        "user_question": user_question,
        "sql": sql,
        "result": result_rows,
        "note": "Отвечай на языке вопроса",
    }
    return json.dumps(payload, ensure_ascii=False)


async def generate_answer(user_question: str, sql: str, result_rows: List[Dict[str, Any]]) -> Dict[str, str]:
    input_text = _format_input(user_question or "", sql or "", result_rows or [])
    try:
        kwargs = _build_kwargs(input_text)
        resp = await client.responses.create(**kwargs)
        text = getattr(resp, "output_text", "") or ""
        data = _safe_json_loads(text)
        answer = str(data.get("answer") or "").strip()
        analysis = str(data.get("analysis") or "").strip()
        if not answer:
            answer = "Не удалось сформировать ответ"
        return {"answer": answer, "analysis": analysis}
    except Exception as exc:
        logger.error("Аналитик-агент: ошибка Responses API: %s", exc)
        return {"answer": "Произошла ошибка при формировании ответа.", "analysis": ""}


