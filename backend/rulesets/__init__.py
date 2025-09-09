import os
import glob
from services.detector import load_rules_from_yaml, Rule

def get_builtin_rules() -> list[Rule]:
    """
    Загружает базовые правила из builtin.yaml.
    """
    path = os.path.join(os.path.dirname(__file__), 'builtin.yaml')
    return load_rules_from_yaml(path)

def get_all_yaml_rules() -> list[Rule]:
    """
    Загружает все правила из всех YAML-файлов в каталоге rulesets.
    """
    rules = []
    for yaml_file in glob.glob(os.path.join(os.path.dirname(__file__), '*.yaml')):
        rules.extend(load_rules_from_yaml(yaml_file))
    return rules

rules = get_builtin_rules()