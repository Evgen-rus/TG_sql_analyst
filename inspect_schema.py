import os
import json
import argparse
import sqlite3
from typing import Dict, Any, List

from config import DB_PATH, logger


def _to_uri_readonly(path: str) -> str:
    return f"file:{os.path.abspath(path)}?mode=ro"


def _list_objects(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT type, name, tbl_name
        FROM sqlite_master
        WHERE type IN ('table','view')
          AND name NOT LIKE 'sqlite_%'
        ORDER BY type, name
        """
    )
    rows = cur.fetchall()
    return [ {"type": r[0], "name": r[1], "tbl_name": r[2]} for r in rows ]


def _columns(conn: sqlite3.Connection, table: str) -> List[Dict[str, Any]]:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info('{table}')")
    cols = []
    for cid, name, ctype, notnull, dflt_value, pk in cur.fetchall():
        cols.append({
            "name": name,
            "type": ctype,
            "notnull": bool(notnull),
            "default": dflt_value,
            "pk": bool(pk),
        })
    return cols


def _foreign_keys(conn: sqlite3.Connection, table: str) -> List[Dict[str, Any]]:
    cur = conn.cursor()
    cur.execute(f"PRAGMA foreign_key_list('{table}')")
    fks = []
    for (_id, seq, ref_table, from_col, to_col, on_update, on_delete, match) in cur.fetchall():
        fks.append({
            "table": ref_table,
            "from": from_col,
            "to": to_col,
            "on_update": on_update,
            "on_delete": on_delete,
            "match": match,
        })
    return fks


def _indexes(conn: sqlite3.Connection, table: str) -> List[Dict[str, Any]]:
    cur = conn.cursor()
    cur.execute(f"PRAGMA index_list('{table}')")
    idx_defs = []
    for seq, name, unique, origin, partial in cur.fetchall():
        # читаем колонки индекса
        cur.execute(f"PRAGMA index_info('{name}')")
        cols = [ r[2] for r in cur.fetchall() ]
        idx_defs.append({
            "name": name,
            "unique": bool(unique),
            "columns": cols,
            "origin": origin,
            "partial": bool(partial),
        })
    return idx_defs


def inspect(database_path: str) -> Dict[str, Any]:
    uri = _to_uri_readonly(database_path)
    conn = sqlite3.connect(uri, uri=True)
    try:
        result: Dict[str, Any] = {
            "database_path": os.path.abspath(database_path),
            "objects": [],
        }
        for obj in _list_objects(conn):
            name = obj["name"]
            entry = {
                "type": obj["type"],
                "name": name,
            }
            if obj["type"] == "table":
                entry["columns"] = _columns(conn, name)
                entry["foreign_keys"] = _foreign_keys(conn, name)
                entry["indexes"] = _indexes(conn, name)
            result["objects"].append(entry)
        return result
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Инспекция схемы SQLite в JSON")
    parser.add_argument("--db", dest="db", default=DB_PATH, help="Путь к БД (по умолчанию из config.DB_PATH)")
    args = parser.parse_args()

    try:
        info = inspect(args.db)
        print(json.dumps(info, ensure_ascii=False, indent=2))
    except Exception as exc:
        logger.error("Ошибка инспекции БД: %s", exc)
        raise


if __name__ == "__main__":
    main()


