from typing import Dict, Tuple, List
import os
import time
import sqlite3

from config import DB_PATH, logger


_CACHE_TTL_SEC = 300
_cache_data: Dict[str, str] = {}
_cache_loaded_at: float = 0.0


def _to_uri_readonly(db_path: str) -> str:
    abs_path = os.path.abspath(db_path)
    return f"file:{abs_path}?mode=ro"


def _load_mapping_from_db() -> Dict[str, str]:
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
        for tag, code in rows:
            if not tag or not code:
                continue
            tag_l = str(tag).strip().lower()
            code_u = str(code).strip()
            if tag_l:
                mapping[tag_l] = code_u
            # также даём возможность распознать сам code как code
            mapping[code_u.lower()] = code_u
        return mapping
    finally:
        conn.close()


def get_projects_mapping() -> Dict[str, str]:
    global _cache_data, _cache_loaded_at
    now = time.time()
    if _cache_data and (now - _cache_loaded_at) < _CACHE_TTL_SEC:
        return _cache_data
    try:
        _cache_data = _load_mapping_from_db()
        _cache_loaded_at = now
        return _cache_data
    except Exception as exc:
        logger.error("Не удалось загрузить маппинг projects: %s", exc)
        return _cache_data or {}


def resolve_project_code_from_text(text: str) -> str:
    if not text:
        return ""
    text_l = text.lower()
    mapping = get_projects_mapping()
    # простое эвристическое вхождение
    for key, code in mapping.items():
        if key and key in text_l:
            return code
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


