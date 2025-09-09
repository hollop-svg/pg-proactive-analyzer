from fastapi import FastAPI, Query, UploadFile, File, HTTPException, Body, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import psycopg2
from adapters.stats import collect_all_metrics
from adapters.locks import collect_lock_metrics
from services.advisor import advise_query, compare_plans, extract_plan_metrics
from metrics import METRIC_KEYS
from services.detector import load_rules_from_yaml, Rule
import os
import tempfile
import shutil
import json
from typing import List, Optional
from datetime import datetime
from decimal import Decimal

def convert_decimals(obj):
    if isinstance(obj, dict):
        return {k: convert_decimals(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_decimals(i) for i in obj]
    elif isinstance(obj, Decimal):
        return float(obj)
    else:
        return obj

hacaton = FastAPI(title="PostgreSQL Query Guard API")

hacaton.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

HISTORY_FILE = "optimization_history.json"
DEFAULT_CONNECTION_PARAMS = None

class DBConnectionParams(BaseModel):
    host: str = "localhost"
    port: int = 5432
    user: str = "postgres"
    password: str = ""
    dbname: str = "postgres"

class QueryRequest(BaseModel):
    query: str
    connection: Optional[DBConnectionParams] = None

class CompareRequest(BaseModel):
    before_query: str
    after_query: str
    connection: DBConnectionParams

class HistoryRecord(BaseModel):
    date: str
    query: str
    action: str
    result: str
    before_metrics: Optional[dict] = None
    after_metrics: Optional[dict] = None

def get_conn(params: DBConnectionParams):
    return psycopg2.connect(
        host=params.host,
        port=params.port,
        user=params.user,
        password=params.password,
        dbname=params.dbname
    )

def load_history() -> List[dict]:
    if not os.path.exists(HISTORY_FILE):
        return []
    with open(HISTORY_FILE, "r", encoding="utf-8") as f:
        content = f.read().strip()
        if not content:
            return []
        return json.loads(content)

def save_history(history: list):
    with open(HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(convert_decimals(history), f, ensure_ascii=False, indent=2)

@hacaton.get("/history")
def get_history():
    return {"history": load_history()}

@hacaton.get("/dbinfo")
def get_db_info():
    global DEFAULT_CONNECTION_PARAMS
    if DEFAULT_CONNECTION_PARAMS:
        params = DBConnectionParams(**DEFAULT_CONNECTION_PARAMS)
    else:
        params = DBConnectionParams()
    conn = get_conn(params)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT current_database();")
            dbname = cur.fetchone()[0]
            cur.execute("SELECT pg_database_size(current_database());")
            dbsize = cur.fetchone()[0]
            cur.execute("SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public';")
            tables_count = cur.fetchone()[0]
            cur.execute("SELECT count(*) FROM pg_user;")
            users_count = cur.fetchone()[0]
            cur.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public';")
            tables = [r[0] for r in cur.fetchall()]
            tables_info = []
            for tablename in tables:
                cur.execute(f"SELECT pg_total_relation_size('public.\"{tablename}\"');")
                tsize = cur.fetchone()[0]
                cur.execute(f"SELECT indexname, indexdef FROM pg_indexes WHERE tablename = %s;", (tablename,))
                indexes = [{"name": idx[0], "def": idx[1]} for idx in cur.fetchall()]
                cur.execute("""
                    SELECT last_vacuum, last_autovacuum, last_analyze, last_autoanalyze
                    FROM pg_stat_user_tables WHERE relname = %s;
                """, (tablename,))
                stat = cur.fetchone()
                last_update = max([d for d in stat if d is not None]) if stat else None
                tables_info.append({
                    "name": tablename,
                    "size": tsize,
                    "indexes": indexes,
                    "last_update": last_update
                })
        return {
            "dbname": dbname,
            "dbsize": dbsize,
            "tables_count": tables_count,
            "users_count": users_count,
            "tables": tables_info
        }
    finally:
        conn.close()

@hacaton.post("/history")
def add_history(record: HistoryRecord):
    history = load_history()
    history.insert(0, record.dict())
    save_history(history)
    return {"status": "ok", "record": record}

@hacaton.post("/analyze")
def analyze_query(req: QueryRequest):
    global DEFAULT_CONNECTION_PARAMS
    if req.connection:
        conn_params = req.connection
    elif DEFAULT_CONNECTION_PARAMS:
        conn_params = DBConnectionParams(**DEFAULT_CONNECTION_PARAMS)
    else:
        conn_params = DBConnectionParams()
    conn = get_conn(conn_params)
    try:
        plan = get_explain_plan(conn, req.query)
        advice = advise_query(plan)
        metrics = collect_all_metrics(conn, conn_params.dbname, req.query)
        lock_metrics = collect_lock_metrics(conn)

        for a in advice['advice']:
            fix_ddl = a.get('fix_ddl')
            if fix_ddl:
                with conn.cursor() as cur:
                    try:
                        cur.execute("BEGIN;")
                        cur.execute(fix_ddl)
                        alt_plan = get_explain_plan(conn, req.query)
                        cur.execute("ROLLBACK;")
                        a['metrics_before'] = a['metrics']
                        a['metrics_after'] = extract_plan_metrics(alt_plan)
                        a['improvement'] = compare_plans(plan, alt_plan)['improvement']
                    except Exception as e:
                        a['metrics_after'] = None
                        a['improvement'] = None
                        a['error'] = str(e)

        record = {
            "date": datetime.utcnow().isoformat(),
            "query": req.query,
            "advice": advice['advice'],
            "metrics": metrics,
            "locks": lock_metrics,
        }
        history = load_history()
        history.insert(0, record)
        save_history(history)
        return record
    finally:
        conn.close()

@hacaton.get("/metrics")
def get_metrics():
    """
    Получить список поддерживаемых метрик.
    """
    return {"metrics": METRIC_KEYS}

@hacaton.post("/rules/upload")
async def upload_rules(file: UploadFile = File(...)):
    """
    Загрузить кастомные правила (YAML).
    """
    if not file.filename.endswith('.yaml'):
        raise HTTPException(status_code=400, detail="Требуется YAML-файл")
    with tempfile.NamedTemporaryFile(delete=False, suffix='.yaml') as tmp:
        shutil.copyfileobj(file.file, tmp)
        tmp_path = tmp.name
    try:
        rules = load_rules_from_yaml(tmp_path)
        return {"status": "ok", "rules_loaded": [r.name for r in rules]}
    finally:
        os.remove(tmp_path)

@hacaton.get("/heatmap")
def get_heatmap():
    history = load_history()
    issues = {}
    by_table = {}
    by_hour = {}
    for rec in history:
        advice_list = rec.get("advice", [])
        dt = rec.get("date")
        hour = None
        if dt:
            hour = dt[11:13]
        for a in advice_list:
            issue = a.get("issue")
            table = a.get("metrics", {}).get("relation") or "unknown"
            issues[issue] = issues.get(issue, 0) + 1
            by_table[table] = by_table.get(table, 0) + 1
            if hour:
                by_hour[hour] = by_hour.get(hour, 0) + 1
    return {
        "issues": issues,
        "by_table": by_table,
        "by_hour": by_hour,
    }

@hacaton.get("/health")
def health():
    return {"status": "ok"}

@hacaton.post("/check_connection")
def check_connection(params: DBConnectionParams):
    global DEFAULT_CONNECTION_PARAMS
    try:
        conn = get_conn(params)
        conn.close()
        DEFAULT_CONNECTION_PARAMS = params.dict()
        return {"status": "ok", "message": "Соединение успешно"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

def get_explain_plan(conn, query: str) -> dict:
    with conn.cursor() as cur:
        cur.execute(f"EXPLAIN (FORMAT JSON) {query}")
        plan = cur.fetchone()[0][0]['Plan']
        return plan