from typing import Dict, Any, List, Optional
from metrics import make_metrics_dict
from services.detector import collect_flags, Rule

Plan = Dict[str, Any]
Flag = Dict[str, Any]
Advice = Dict[str, Any]

def extract_plan_metrics(plan: Plan) -> Dict[str, Any]:
    return make_metrics_dict(
        cost=plan.get('Total Cost', 0),
        rows=plan.get('Plan Rows', 0),
        width=plan.get('Plan Width', 0),
        startup_cost=plan.get('Startup Cost', 0),
        actual_time=plan.get('Actual Total Time', None),
        actual_rows=plan.get('Actual Rows', None),
    )

def extract_placeholders(plan: Plan) -> Dict[str, str]:
    """
    Извлекает значения для подстановки в fix_ddl из плана запроса.
    """
    placeholders = {}

    if 'Relation Name' in plan:
        placeholders['relation'] = plan['Relation Name']
    elif 'relation' in plan:
        placeholders['relation'] = plan['relation']
    else:
        placeholders['relation'] = 'table'

    column = None
    for key in ['Index Cond', 'Filter', 'Hash Cond', 'Sort Key']:
        cond = plan.get(key)
        if cond:
            import re
            match = re.search(r'([a-zA-Z_][a-zA-Z0-9_]*)', str(cond))
            if match:
                column = match.group(1)
                break
    placeholders['column'] = column or 'col1'
    placeholders['join_column'] = column or 'col1'
    placeholders['sort_column'] = column or 'col1'

    if 'Node Type' in plan:
        placeholders['node_type'] = plan['Node Type']

    return placeholders

def fill_fix_ddl(fix_ddl: Optional[str], plan: Plan) -> Optional[str]:
    """
    Подставляет значения в fix_ddl из плана.
    """
    if not fix_ddl:
        return None
    placeholders = extract_placeholders(plan)
    try:
        return fix_ddl.format(**placeholders)
    except Exception:
        # Если не удалось подставить — вернуть как есть
        return fix_ddl

def generate_advice(plan: Plan, rules: Optional[List[Rule]] = None) -> List[Advice]:
    flags = collect_flags(plan, rules)
    metrics = extract_plan_metrics(plan)
    advice_list = []
    for flag in flags:
        fix_ddl = None
        if 'fix_ddl' in flag:
            fix_ddl = fill_fix_ddl(flag['fix_ddl'], plan)
        advice = {
            'issue': flag['type'],
            'recommendation': flag['recommendation'],
            'priority': flag['priority'],
            'metrics': metrics,
            'fix_ddl': fix_ddl
        }
        advice_list.append(advice)
    return advice_list

def compare_plans(before: Plan, after: Plan) -> Dict[str, Any]:
    before_metrics = extract_plan_metrics(before)
    after_metrics = extract_plan_metrics(after)
    improvement = make_metrics_dict(
        cost=(before_metrics.get('cost', 0) or 0) - (after_metrics.get('cost', 0) or 0),
        rows=(before_metrics.get('rows', 0) or 0) - (after_metrics.get('rows', 0) or 0),
        actual_time=(before_metrics.get('actual_time', 0) or 0) - (after_metrics.get('actual_time', 0) or 0),
    )
    return {
        'before': before_metrics,
        'after': after_metrics,
        'improvement': improvement,
    }

def advise_query(plan: Plan, alt_plan: Optional[Plan] = None) -> Dict[str, Any]:
    advice = generate_advice(plan)
    comparison = None
    if alt_plan:
        comparison = compare_plans(plan, alt_plan)
    return {
        'advice': advice,
        'comparison': comparison,
    }