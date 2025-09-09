import psycopg2
from typing import Dict, Any, List, Optional

def get_current_locks(conn) -> List[Dict[str, Any]]:
    """
    Возвращает список текущих блокировок в базе.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                pid,
                locktype,
                relation::regclass AS relation,
                mode,
                granted,
                fastpath,
                virtualtransaction,
                transactionid,
                virtualxid,
                database,
                application_name,
                state,
                query,
                now() - query_start AS query_duration
            FROM pg_locks
            LEFT JOIN pg_stat_activity USING (pid)
            WHERE relation IS NOT NULL
            ORDER BY granted DESC, query_duration DESC
        """)
        columns = [desc[0] for desc in cur.description]
        return [dict(zip(columns, row)) for row in cur.fetchall()]

def get_blocked_processes(conn) -> List[Dict[str, Any]]:
    """
    Возвращает процессы, которые ждут блокировки (не granted).
    """
    locks = get_current_locks(conn)
    return [lock for lock in locks if not lock['granted']]

def get_lock_stats(conn) -> Dict[str, Any]:
    """
    Возвращает агрегированные метрики по блокировкам.
    """
    locks = get_current_locks(conn)
    blocked = [l for l in locks if not l['granted']]
    by_table = {}
    for lock in blocked:
        rel = lock['relation']
        by_table.setdefault(rel, 0)
        by_table[rel] += 1
    return {
        "total_locks": len(locks),
        "blocked_count": len(blocked),
        "blocked_tables": by_table,
    }

def detect_long_locks(conn, threshold_seconds: int = 10) -> List[Dict[str, Any]]:
    """
    Находит блокировки, которые держатся дольше threshold_seconds.
    """
    locks = get_current_locks(conn)
    long_locks = [
        lock for lock in locks
        if lock['granted'] and lock['query_duration'] and lock['query_duration'].total_seconds() > threshold_seconds
    ]
    return long_locks

def get_deadlocks(conn) -> List[Dict[str, Any]]:
    """
    Находит процессы, которые могут быть вовлечены в deadlock (грубо).
    """
    # В реальности deadlock-ы ловятся по логам или pg_stat_activity, но можно попытаться найти циклы ожидания
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                a.pid AS waiting_pid,
                a.query AS waiting_query,
                a.state AS waiting_state,
                b.pid AS blocking_pid,
                b.query AS blocking_query,
                b.state AS blocking_state
            FROM pg_locks bl
            JOIN pg_stat_activity a ON bl.pid = a.pid
            JOIN pg_locks wl ON bl.locktype = wl.locktype
                AND bl.database IS NOT DISTINCT FROM wl.database
                AND bl.relation IS NOT DISTINCT FROM wl.relation
                AND bl.page IS NOT DISTINCT FROM wl.page
                AND bl.tuple IS NOT DISTINCT FROM wl.tuple
                AND bl.virtualxid IS NOT DISTINCT FROM wl.virtualxid
                AND bl.transactionid IS NOT DISTINCT FROM wl.transactionid
                AND bl.classid IS NOT DISTINCT FROM wl.classid
                AND bl.objid IS NOT DISTINCT FROM wl.objid
                AND bl.objsubid IS NOT DISTINCT FROM wl.objsubid
            JOIN pg_stat_activity b ON wl.pid = b.pid
            WHERE NOT bl.granted AND wl.granted
        """)
        columns = [desc[0] for desc in cur.description]
        return [dict(zip(columns, row)) for row in cur.fetchall()]

# Основная точка входа для сбора всех метрик по блокировкам
def collect_lock_metrics(conn) -> Dict[str, Any]:
    return {
        "lock_stats": get_lock_stats(conn),
        "blocked_processes": get_blocked_processes(conn),
        "long_locks": detect_long_locks(conn),
        "deadlocks": get_deadlocks(conn),
    }