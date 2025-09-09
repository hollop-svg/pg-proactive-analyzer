import psycopg2
from typing import Dict, Any
from metrics import make_metrics_dict

def get_query_cost(conn, query: str) -> float:
    with conn.cursor() as cur:
        cur.execute(f"EXPLAIN (FORMAT JSON) {query}")
        plan = cur.fetchone()[0][0]['Plan']
        return plan.get('Total Cost', 0)

def get_query_result_volume(conn, query: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f"EXPLAIN (FORMAT JSON) {query}")
        plan = cur.fetchone()[0][0]['Plan']
        return plan.get('Plan Rows', 0)

def get_cache_hit_ratio(conn) -> float:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                CASE WHEN sum(blks_hit + blks_read) = 0 THEN 1
                ELSE sum(blks_hit)::float / sum(blks_hit + blks_read)
                END
            FROM pg_stat_database
        """)
        return cur.fetchone()[0]

def get_index_usage(conn) -> float:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                CASE WHEN sum(seq_scan + idx_scan) = 0 THEN 1
                ELSE sum(idx_scan)::float / sum(seq_scan + idx_scan)
                END
            FROM pg_stat_user_tables
        """)
        return cur.fetchone()[0]

def get_wait_time(conn) -> int:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT count(*) FROM pg_stat_activity WHERE wait_event_type IS NOT NULL
        """)
        return cur.fetchone()[0]


def get_disk_io(conn) -> Dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'pg_stat_database'
        """)
        columns = [r[0] for r in cur.fetchall()]
        if 'blks_written' in columns:
            cur.execute("""
                SELECT sum(blks_read) as blocks_read, sum(blks_written) as blocks_written
                FROM pg_stat_database
            """)
            row = cur.fetchone()
            return {"disk_io_read": row[0], "disk_io_write": row[1]}
        else:
            cur.execute("""
                SELECT sum(blks_read) as blocks_read
                FROM pg_stat_database
            """)
            row = cur.fetchone()
            return {"disk_io_read": row[0], "disk_io_write": None}

def get_database_size(conn, dbname: str) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT pg_database_size(%s)", (dbname,))
        return cur.fetchone()[0]

def get_deadlock_count(conn) -> int:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT sum(deadlocks) FROM pg_stat_database
        """)
        return cur.fetchone()[0]

def get_uptime(conn) -> float:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT extract(epoch FROM now() - pg_postmaster_start_time())
            FROM pg_postmaster_start_time()
        """)
        return cur.fetchone()[0]

def get_active_connections(conn) -> int:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT count(*) FROM pg_stat_activity WHERE state = 'active'
        """)
        return cur.fetchone()[0]

def get_lock_contention(conn) -> float:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT count(*) FROM pg_locks WHERE granted = false
        """)
        return cur.fetchone()[0]

def get_replication_lag(conn) -> float:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT CASE WHEN pg_last_wal_receive_lsn() IS NULL THEN 0
            ELSE pg_wal_lsn_diff(pg_current_wal_lsn(), pg_last_wal_receive_lsn())
            END
        """)
        return cur.fetchone()[0]

def collect_all_metrics(conn, dbname: str, query: str = None) -> Dict[str, Any]:
    disk_io = get_disk_io(conn)
    metrics = make_metrics_dict(
        cost=get_query_cost(conn, query) if query else None,
        rows=get_query_result_volume(conn, query) if query else None,
        cache_hit_ratio=get_cache_hit_ratio(conn),
        index_usage=get_index_usage(conn),
        wait_time=get_wait_time(conn),
        disk_io_read=disk_io["disk_io_read"],
        disk_io_write=disk_io["disk_io_write"],
        database_size=get_database_size(conn, dbname),
        deadlock_count=get_deadlock_count(conn),
        uptime=get_uptime(conn),
        active_connections=get_active_connections(conn),
        lock_contention=get_lock_contention(conn),
        replication_lag=get_replication_lag(conn),
    )
    return metrics