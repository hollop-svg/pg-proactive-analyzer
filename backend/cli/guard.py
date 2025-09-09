import argparse
import os
import sys
import psycopg2
import json

from adapters.stats import collect_all_metrics
from adapters.locks import collect_lock_metrics
from services.advisor import advise_query
from adapters.planner import get_explain_plan

def parse_args():
    parser = argparse.ArgumentParser(description="PostgreSQL Query Guard")
    parser.add_argument('--host', default=os.getenv('PGHOST', 'localhost'))
    parser.add_argument('--port', type=int, default=int(os.getenv('PGPORT', 5432)))
    parser.add_argument('--user', default=os.getenv('PGUSER', 'postgres'))
    parser.add_argument('--password', default=os.getenv('PGPASSWORD', ''))
    parser.add_argument('--dbname', default=os.getenv('PGDATABASE', 'postgres'))
    parser.add_argument('--query', help='SQL query to analyze')
    parser.add_argument('--query-file', help='Path to file with SQL query')
    parser.add_argument('--output', choices=['json', 'md', 'log'], default='json')
    parser.add_argument('--fail-on-high', action='store_true', help='Exit with error if high-priority flags found')
    return parser.parse_args()

def read_query(args):
    if args.query:
        return args.query
    elif args.query_file:
        with open(args.query_file, 'r', encoding='utf-8') as f:
            return f.read()
    else:
        print("Error: No query provided", file=sys.stderr)
        sys.exit(2)

def main():
    args = parse_args()
    query = read_query(args)

    # Подключение к БД
    conn = psycopg2.connect(
        host=args.host, port=args.port,
        user=args.user, password=args.password,
        dbname=args.dbname
    )

    # Получение плана выполнения
    plan = get_explain_plan(conn, query)  # реализуй функцию, возвращающую dict из EXPLAIN (FORMAT JSON)

    # Анализ запроса
    advice = advise_query(plan)

    # Сбор метрик
    metrics = collect_all_metrics(conn, args.dbname, query)
    lock_metrics = collect_lock_metrics(conn)

    # Формирование результата
    result = {
        "query": query,
        "advice": advice,
        "metrics": metrics,
        "locks": lock_metrics,
    }

    # Вывод
    if args.output == 'json':
        print(json.dumps(result, indent=2, ensure_ascii=False))
    elif args.output == 'md':
        print(render_markdown(result))
    else:
        print(render_log(result))

    # Коды возврата
    if args.fail_on_high:
        high_flags = [a for a in advice['advice'] if a['priority'] == 'high']
        if high_flags:
            sys.exit(1)
    sys.exit(0)

def render_markdown(result):
    # Простой markdown-вывод (можно доработать)
    md = f"## Query\n{result['query']}\n"
    md += "## Advice\n"
    for a in result['advice']['advice']:
        md += f"- {a['issue']} ({a['priority']}): {a['recommendation']}\n"
    md += "\n## Metrics\n"
    for k, v in result['metrics'].items():
        md += f"- {k}: {v}\n"
    md += "\n## Locks\n"
    md += f"- Blocked: {result['locks']['lock_stats']['blocked_count']}\n"
    return md

def render_log(result):
    # Цветной лог (можно использовать colorama)
    for a in result['advice']['advice']:
        print(f"[{a['priority'].upper()}] {a['issue']}: {a['recommendation']}")
    print("Metrics:", result['metrics'])
    print("Locks:", result['locks']['lock_stats'])
    return ""

if __name__ == "__main__":
    main()