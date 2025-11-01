from typing import Dict, Any, List
import json
from openai import AsyncOpenAI

from config import OPENAI_API_KEY, OPENAI_MODEL, OPENAI_PARAMS, logger
from prompts import ANALYST_SYSTEM_PROMPT
from project_resolver import get_code_to_tag_map


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


def _format_input(user_question: str, sql: str, result_rows: List[Dict[str, Any]], code_names: Dict[str, str]) -> str:
    payload = {
        "user_question": user_question,
        "sql": sql,
        "result": result_rows,
        "project_names": code_names,  # { '[LR166]': '[LR166] ПромСпецАвто Татьяна' }
        "note": "Отвечай на языке вопроса. В ответе показывай человеко-понятные названия проектов (project_names), а не только коды.",
    }
    return json.dumps(payload, ensure_ascii=False)


async def generate_answer(user_question: str, sql: str, result_rows: List[Dict[str, Any]]) -> Dict[str, str]:
    # построим соответствия кодов из результата -> имена проектов
    code2name = {}
    try:
        reverse_map = get_code_to_tag_map()  # 'LR166' -> '[LR166] ПромСпецАвто Татьяна'
        seen = set()
        for row in result_rows or []:
            code = str(row.get("project_code") or "").strip()
            if not code or code in seen:
                continue
            seen.add(code)
            code_clean = code
            if code_clean.startswith('[') and code_clean.endswith(']'):
                code_clean = code_clean[1:-1]
            name = reverse_map.get(code_clean, "")
            if name:
                code2name[code] = name
    except Exception:
        pass

    input_text = _format_input(user_question or "", sql or "", result_rows or [], code2name)
    try:
        kwargs = _build_kwargs(input_text)
        resp = await client.responses.create(**kwargs)
        text = getattr(resp, "output_text", "") or ""
        data = _safe_json_loads(text)
        answer = str(data.get("answer") or "").strip()
        analysis = str(data.get("analysis") or "").strip()
        # Постобработка: подставим имена проектов в формат "[CODE] Имя"
        if code2name:
            for code, name in code2name.items():
                if not code or not name:
                    continue
                # если name уже начинается с [CODE], не дублируем
                display = name if name.startswith(code) else f"{code} {name}"
                if code in answer and display not in answer:
                    answer = answer.replace(code, display)
                if code in analysis and display not in analysis:
                    analysis = analysis.replace(code, display)
        if not answer:
            answer = "Не удалось сформировать ответ"
        return {"answer": answer, "analysis": analysis}
    except Exception as exc:
        logger.error("Аналитик-агент: ошибка Responses API: %s", exc)
        return {"answer": "Произошла ошибка при формировании ответа.", "analysis": ""}


