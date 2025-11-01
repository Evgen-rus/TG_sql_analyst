from typing import Dict, Any
import json
from openai import AsyncOpenAI

from config import OPENAI_API_KEY, OPENAI_MODEL, OPENAI_PARAMS, logger
from prompts import SQL_AGENT_SYSTEM_PROMPT
from project_resolver import build_mapping_context, resolve_project_code_from_text


client = AsyncOpenAI(api_key=OPENAI_API_KEY)


def _build_kwargs(input_text: str) -> Dict[str, Any]:
    kwargs: Dict[str, Any] = {
        "model": OPENAI_MODEL,
        "input": input_text,
        "instructions": SQL_AGENT_SYSTEM_PROMPT,
    }
    max_tokens = OPENAI_PARAMS.get("max_tokens")
    if isinstance(max_tokens, int) and max_tokens > 0:
        kwargs["max_output_tokens"] = max_tokens
    return kwargs


def _safe_json_loads(text: str) -> Dict[str, Any]:
    try:
        return json.loads(text)
    except Exception:
        # Попробуем вычленить первый JSON-объект по фигурным скобкам
        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end != -1 and end > start:
            try:
                return json.loads(text[start:end+1])
            except Exception:
                pass
    return {}


async def generate_sql(question: str) -> Dict[str, str]:
    if not question or not isinstance(question, str):
        return {"sql": "", "explanation": "Пустой запрос пользователя"}

    try:
        # Добавим контекст соответствий проектов.
        mapping_block = build_mapping_context()
        resolved_code = resolve_project_code_from_text(question)
        enriched_input = (
            "USER_QUESTION:\n" + question.strip() + "\n\n"
            "PROJECTS_MAPPING (tag/name -> project_code):\n" + (mapping_block or "- (нет записей)") + "\n\n"
            "Правило: если в вопросе есть название/тег проекта, используй строго leads.project_code='<code>'\n"
            "Не используй JOIN, только таблицу leads.\n"
        )
        if resolved_code:
            enriched_input += f"Hint: resolved_project_code={resolved_code}\n"

        kwargs = _build_kwargs(enriched_input)
        resp = await client.responses.create(**kwargs)
        text = getattr(resp, "output_text", "") or ""
        data = _safe_json_loads(text)
        sql = str(data.get("sql") or "").strip()
        explanation = str(data.get("explanation") or "").strip()
        if not sql:
            explanation = explanation or ("Не удалось распознать SQL из ответа модели: " + text[:200])
        return {"sql": sql, "explanation": explanation}
    except Exception as exc:
        logger.error("SQL-агент: ошибка Responses API: %s", exc)
        return {"sql": "", "explanation": "Ошибка при генерации SQL"}


