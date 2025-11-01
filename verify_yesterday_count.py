import argparse
from typing import Optional

from db import execute_select
from project_resolver import resolve_project_code_from_text
from config import logger


def main() -> None:
    parser = argparse.ArgumentParser(description="Проверка: сколько лидов было вчера по проекту")
    parser.add_argument("--project", dest="project", default="", help="Название/тег проекта (например, 'пром спец авто')")
    parser.add_argument("--code", dest="code", default="", help="Точный project_code (например, 'LR166')")
    parser.add_argument("--localtime", action="store_true", help="Использовать date('now','localtime','-1 day') вместо UTC")
    parser.add_argument("--contains", action="store_true", help="Искать по вхождению кода (LIKE %CODE%) вместо точного '[CODE]'")
    parser.add_argument("--samples", type=int, default=0, help="Сколько примеров строк вывести (0 = не выводить)")
    args = parser.parse_args()

    project_code: Optional[str] = args.code.strip() or ""
    if not project_code:
        if not args.project:
            raise SystemExit("Укажите --project или --code")
        project_code = resolve_project_code_from_text(args.project or "")
        if not project_code:
            raise SystemExit("Не удалось определить project_code из названия. Укажите --code явно.")

    date_expr = "date('now','localtime','-1 day')" if args.localtime else "date('now','-1 day')"

    # По умолчанию точное сравнение по '[CODE]'
    if not args.contains:
        bracketed = project_code if (project_code.startswith("[") and project_code.endswith("]")) else f"[{project_code}]"
        count_sql = (
            f"SELECT COUNT(*) AS cnt FROM leads "
            f"WHERE project_code = ? AND date(created_at) = {date_expr}"
        )
        res = execute_select(count_sql, params=(bracketed,))
    else:
        like_pat = f"%{project_code}%"
        count_sql = (
            f"SELECT COUNT(*) AS cnt FROM leads "
            f"WHERE project_code LIKE ? AND date(created_at) = {date_expr}"
        )
        res = execute_select(count_sql, params=(like_pat,))
    cnt = int(res[0]["cnt"]) if res else 0
    print(f"project_code={project_code}; yesterday_count={cnt}; date_expr={date_expr}; mode={'LIKE' if args.contains else 'EQUALS_BRACKETED'}")

    if args.samples and cnt > 0:
        if not args.contains:
            bracketed = project_code if (project_code.startswith("[") and project_code.endswith("]")) else f"[{project_code}]"
            sample_sql = (
                f"SELECT id, created_at, project_code, gck_tag, project_tag, check_mark "
                f"FROM leads WHERE project_code = ? AND date(created_at) = {date_expr} "
                f"ORDER BY datetime(created_at) DESC LIMIT ?"
            )
            rows = execute_select(sample_sql, params=(bracketed, args.samples))
        else:
            like_pat = f"%{project_code}%"
            sample_sql = (
                f"SELECT id, created_at, project_code, gck_tag, project_tag, check_mark "
                f"FROM leads WHERE project_code LIKE ? AND date(created_at) = {date_expr} "
                f"ORDER BY datetime(created_at) DESC LIMIT ?"
            )
            rows = execute_select(sample_sql, params=(like_pat, args.samples))
        print("samples:")
        for r in rows:
            print(r)


if __name__ == "__main__":
    main()


