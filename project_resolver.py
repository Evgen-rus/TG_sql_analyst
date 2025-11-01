from typing import Dict, Tuple, List
import os
import time
import sqlite3

from config import DB_PATH, logger


_CACHE_TTL_SEC = 300
_cache_data: Dict[str, str] = {}
_cache_code2tag: Dict[str, str] = {}
_cache_loaded_at: float = 0.0


def _to_uri_readonly(db_path: str) -> str:
    abs_path = os.path.abspath(db_path)
    return f"file:{abs_path}?mode=ro"


def _load_mapping_from_db() -> tuple[Dict[str, str], Dict[str, str]]:
    uri = _to_uri_readonly(DB_PATH)
    conn = sqlite3.connect(uri, uri=True)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT project_tag, project_code
            FROM projects
            WHERE project_tag IS NOT NULL AND project_code IS NOT NULL
            """
        )
        rows = cur.fetchall()
        mapping: Dict[str, str] = {}
        code2tag: Dict[str, str] = {}
        for tag, code in rows:
            if not tag or not code:
                continue
            tag_clean = str(tag).strip()
            tag_l = tag_clean.lower()
            code_u = str(code).strip()  # из БД приходит со скобками, например "[LR165]"

            # 1) mapping: ключ — tag/имя (lower), значение — code из БД (со скобками)
            if tag_l:
                mapping[tag_l] = code_u
            # также позволим распознавать сам code как ключ (lower) → вернуть canonical code со скобками
            mapping[code_u.lower()] = code_u

            # 2) code2tag: только ключ СО скобками (канонический формат)
            if code_u and code_u not in code2tag:
                code2tag[code_u] = tag_clean
        return mapping, code2tag
    finally:
        conn.close()


def get_projects_mapping() -> Dict[str, str]:
    global _cache_data, _cache_loaded_at
    now = time.time()
    if _cache_data and (now - _cache_loaded_at) < _CACHE_TTL_SEC:
        return _cache_data
    try:
        _cache_data, _cache_code2tag = _load_mapping_from_db()
        _cache_loaded_at = now
        try:
            logger.info("Projects mapping loaded: tags→codes=%s, codes→tags=%s", len(_cache_data or {}), len(_cache_code2tag or {}))
        except Exception:
            pass
        return _cache_data
    except Exception as exc:
        logger.error("Не удалось загрузить маппинг projects: %s", exc)
        return _cache_data or {}


def get_code_to_tag_map() -> Dict[str, str]:
    global _cache_code2tag, _cache_loaded_at
    now = time.time()
    if _cache_code2tag and (now - _cache_loaded_at) < _CACHE_TTL_SEC:
        return _cache_code2tag
    # попытка перезагрузить из БД, если основной маппинг протух
    try:
        mapping, code2tag = _load_mapping_from_db()
        # обновим оба кэша, чтобы синхронизировать времена
        globals()['_cache_data'] = mapping
        _cache_code2tag = code2tag
        globals()['_cache_loaded_at'] = now
        return _cache_code2tag
    except Exception as exc:
        logger.error("Не удалось загрузить code->tag mapping: %s", exc)
        return _cache_code2tag or {}


def get_tag_by_code(code: str) -> str:
    if not code:
        return ""
    key = code.strip()
    # Ищем канонический ключ со скобками
    value = get_code_to_tag_map().get(key, "")
    if value:
        return value
    # Фолбэк: если пришёл код без скобок — обернём
    if not (key.startswith('[') and key.endswith(']')):
        wrapped = f"[{key}]"
        return get_code_to_tag_map().get(wrapped, "")
    return ""


def build_mapping_context(max_items: int = 200) -> str:
    mapping = get_projects_mapping()
    # Оставим по одному примеру для каждого code (tag -> code)
    code_to_tag: Dict[str, str] = {}
    for tag_or_code, code in mapping.items():
        # хотим показать осмысленный tag, а не дубликат кода
        if tag_or_code.lower() == code.lower():
            continue
        if code not in code_to_tag:
            code_to_tag[code] = tag_or_code
    lines: List[str] = []
    for idx, (code, tag) in enumerate(code_to_tag.items()):
        if idx >= max_items:
            break
        lines.append(f"- {tag} -> {code}")
    return "\n".join(lines)


