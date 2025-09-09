from __future__ import annotations
from dataclasses import dataclass
from typing import Callable, Dict, List, Any, Iterable, Optional
import os
import yaml

Plan = Dict[str, Any]
Flag = Dict[str, Any]
Predicate = Callable[[Plan], bool]
Builder = Callable[[Plan], Flag]

@dataclass(slots=True)
class Rule:
    name: str
    pred: Predicate
    build: Builder

# ────────────────────────────────────────────────────────────────
# 1.  Загрузка правил из YAML

def load_rules_from_yaml(path: str) -> List[Rule]:
    with open(path, 'r', encoding='utf-8') as f:
        raw_rules = yaml.safe_load(f)
    rules = []
    for r in raw_rules:
        match = r.get('match', {})
        def pred(plan, match=match):
            if 'node_type' in match and plan.get('Node Type') != match['node_type']:
                return False
            if 'node_type_in' in match and plan.get('Node Type') not in match['node_type_in']:
                return False
            if 'plan_rows_gt' in match and plan.get('Plan Rows', 0) <= match['plan_rows_gt']:
                return False
            if 'total_cost_gt' in match and plan.get('Total Cost', 0) <= match['total_cost_gt']:
                return False
            if match.get('filter_absent') and plan.get('Filter'):
                return False
            return True
        def build(plan, r=r):
            rec = r['recommendation']
            # подстановка node_type если надо
            if '{node_type}' in rec:
                rec = rec.format(node_type=plan.get('Node Type'))
            return {
                'type': r['name'],
                'recommendation': rec,
                'priority': r['priority']
            }
        rules.append(Rule(r['name'], pred, build))
    return rules

# ────────────────────────────────────────────────────────────────
# 2.  Загрузка всех правил (можно расширять)

def get_all_rules() -> List[Rule]:
    # путь до builtin.yaml
    yaml_path = os.path.join(os.path.dirname(__file__), '../rulesets/builtin.yaml')
    yaml_path = os.path.abspath(yaml_path)
    rules = load_rules_from_yaml(yaml_path)
    return rules

# ────────────────────────────────────────────────────────────────
# 3.  Проверка одного плана

def detect_red_flags(plan: Plan, rules: Optional[Iterable[Rule]] = None) -> list[Flag]:
    if rules is None:
        rules = get_all_rules()
    return [rule.build(plan) for rule in rules if rule.pred(plan)]

# ────────────────────────────────────────────────────────────────
# 4.  Рекурсивный обход

def walk_plan(plan: Plan, rules: Optional[Iterable[Rule]] = None) -> Iterable[Flag]:
    yield from detect_red_flags(plan, rules)
    for sub in plan.get('Plans', ()):
        yield from walk_plan(sub, rules)

def collect_flags(plan: Plan, rules: Optional[Iterable[Rule]] = None) -> list[Flag]:
    return list(walk_plan(plan, rules))