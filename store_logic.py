"""Store assignment logic for employees.

Two-tier system:
1. Check override table in SQLite first
2. If no override, use thousand-digit rule on employee number
"""

from database import get_overrides_for_employee, THOUSAND_DIGIT_MAP, STORES


def resolve_store(employee_id) -> list[dict]:
    """Return list of {store_name, ratio} for an employee.

    Returns:
        list of dicts, e.g. [{"store_name": "春日", "ratio": 60}, {"store_name": "巣鴨", "ratio": 40}]
        Empty list if employee cannot be resolved.
    """
    try:
        emp_id = int(employee_id)
    except (ValueError, TypeError):
        return []

    # 1. Check override table
    overrides = get_overrides_for_employee(emp_id)
    if overrides:
        return overrides

    # 2. Thousand-digit rule
    if emp_id >= 1000:
        thousand_digit = emp_id // 1000
        store = THOUSAND_DIGIT_MAP.get(thousand_digit)
        if store:
            return [{"store_name": store, "ratio": 100}]

    return []


def apply_ratio(value, ratio: int):
    """Apply a percentage ratio to a numeric value."""
    if value is None:
        return 0
    try:
        return float(value) * ratio / 100
    except (ValueError, TypeError):
        return 0


def get_store_display_name(store: str) -> str:
    """Normalize store display name."""
    # Handle partial matches like 祖師ヶ谷 → 祖師ヶ谷大蔵
    for s in STORES:
        if store in s or s in store:
            return s
    return store
