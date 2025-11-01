from typing import List, Dict, Any, Optional
import os
import sqlite3
import sqlparse
from sqlparse.sql import Statement, Identifier, IdentifierList
from sqlparse import tokens as T

from config import DB_PATH, logger


def _to_uri_readonly(db_path: str) -> str:
    # Преобразуем путь в абсолютный и собираем URI для read-only
    abs_path = os.path.abspath(db_path)
    return f"file:{abs_path}?mode=ro"


def _is_select_statement(stmt: Statement) -> bool:
    try:
        return (stmt.get_type() or "").upper() == "SELECT"
    except Exception:
        return False


def _has_forbidden_tokens(stmt: Statement) -> bool:
    forbidden = {"UPDATE", "INSERT", "DELETE", "DROP", "ALTER", "CREATE", "ATTACH", "DETACH", "REINDEX", "VACUUM", "PRAGMA"}
    for token in stmt.flatten():
        if token.ttype in (T.Keyword, T.Keyword.DDL, T.Keyword.DML):
            if str(token.value).strip().upper() in forbidden:
                return True
    return False


def _extract_table_names(stmt: Statement) -> list:
    tables = []
    tokens = list(stmt.tokens)
    for idx, tok in enumerate(tokens):
        if tok.ttype in (T.Keyword, T.Keyword.DML, T.Keyword.CTE) and str(tok.value).upper() in {"FROM", "JOIN"}:
            # следующий значимый токен может быть Identifier/IdentifierList
            # пропустим пробелы и пунктуацию
            j = idx + 1
            while j < len(tokens) and (tokens[j].is_whitespace or tokens[j].ttype in (T.Punctuation,)):
                j += 1
            if j >= len(tokens):
                continue
            next_tok = tokens[j]
            if isinstance(next_tok, Identifier):
                name = (next_tok.get_real_name() or next_tok.get_name() or str(next_tok).strip()).strip('"`')
                if name:
                    tables.append(name)
            elif isinstance(next_tok, IdentifierList):
                for idf in next_tok.get_identifiers():
                    name = (idf.get_real_name() or idf.get_name() or str(idf).strip()).strip('"`')
                    if name:
                        tables.append(name)
    return tables


def _only_leads_tables(stmt: Statement) -> bool:
    tables = [t.lower() for t in _extract_table_names(stmt)]
    # Разрешаем пустой список (например, SELECT 1) и использование только leads
    return all(t == "leads" for t in tables)


def validate_select_sql(sql: str) -> str:
    if not sql or not isinstance(sql, str):
        raise ValueError("SQL пустой или неверного типа")

    parsed = [s for s in sqlparse.parse(sql) if s and not s.is_whitespace]
    if len(parsed) != 1:
        raise ValueError("Разрешён только один SQL-стейтмент SELECT")

    stmt = parsed[0]
    if _has_forbidden_tokens(stmt):
        raise ValueError("Обнаружены запрещённые операторы в SQL")

    if not _is_select_statement(stmt):
        raise ValueError("Разрешены только SELECT-запросы")

    if not _only_leads_tables(stmt):
        raise ValueError("Разрешено обращаться только к таблице leads")

    # уберём завершающую точку с запятой, если есть
    clean = sql.strip().rstrip(";")
    return clean


def execute_select(sql: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
    query = validate_select_sql(sql)
    uri = _to_uri_readonly(DB_PATH)
    logger.info("Выполняю SQL (read-only): %s", query)

    # URI режим
    conn = sqlite3.connect(uri, uri=True)
    try:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(query, params or tuple())
        rows = cur.fetchall()
        if not rows:
            return []
        columns = rows[0].keys()
        return [ {col: row[col] for col in columns} for row in rows ]
    finally:
        conn.close()


