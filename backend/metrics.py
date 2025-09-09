from typing import Dict, Any, Optional

# Список всех поддерживаемых метрик
METRIC_KEYS = [
    'cost',
    'rows',
    'width',
    'startup_cost',
    'actual_time',
    'actual_rows',
    'cache_hit_ratio',
    'index_usage',
    'wait_time',
    'disk_io_read',
    'disk_io_write',
    'database_size',
    'deadlock_count',
    'uptime',
    'active_connections',
    'lock_contention',
    'replication_lag',
]

def make_metrics_dict(**kwargs) -> Dict[str, Any]:
    """
    Возвращает словарь метрик, заполняя только известные ключи.
    """
    return {k: kwargs.get(k) for k in METRIC_KEYS if k in kwargs}