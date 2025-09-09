import psycopg2
from typing import Dict, Any, Optional, List

def get_explain_plan(
    conn,
    query: str,
    *,
    analyze: bool = False,
    buffers: bool = False,
    settings: bool = False,
    options: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Получить план выполнения запроса (EXPLAIN [ANALYZE] [BUFFERS] [SETTINGS] FORMAT JSON).
    options — временные параметры сессии (например, {'work_mem': '128MB'})
    """
    # 1. Установить временные параметры сессии (если есть)
    if options:
        for k, v in options.items():
            with conn.cursor() as cur:
                cur.execute(f"SET {k} = %s", (v,))
    
    # 2. Собрать EXPLAIN-строку
    explain_opts = []
    if analyze:
        explain_opts.append("ANALYZE")
    if buffers:
        explain_opts.append("BUFFERS")
    if settings:
        explain_opts.append("SETTINGS")
    explain_opts.append("FORMAT JSON")
    explain_str = " ".join(explain_opts)
    
    sql = f"EXPLAIN {explain_str} {query}"
    with conn.cursor() as cur:
        cur.execute(sql)
        plan = cur.fetchone()[0][0]  # FORMAT JSON всегда возвращает список из одного элемента
    return plan

def reset_session_settings(conn):
    """
    Сбросить все параметры сессии к значениям по умолчанию.
    """
    with conn.cursor() as cur:
        cur.execute("RESET ALL")

def compare_plans_with_options(
    conn,
    query: str,
    before_opts: Optional[Dict[str, Any]] = None,
    after_opts: Optional[Dict[str, Any]] = None,
    **kwargs
) -> Dict[str, Any]:
    """
    Получить два плана: до и после применения параметров, для сравнения.
    """
    # План "до"
    reset_session_settings(conn)
    before_plan = get_explain_plan(conn, query, options=before_opts, **kwargs)
    # План "после"
    reset_session_settings(conn)
    after_plan = get_explain_plan(conn, query, options=after_opts, **kwargs)
    # Сбросить настройки после анализа
    reset_session_settings(conn)
    return {
        "before": before_plan,
        "after": after_plan
    }
