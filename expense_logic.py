"""Expense classification logic using keyword matching on 摘要 field."""

from database import get_all_expense_rules


def classify_expense(description: str) -> tuple[str | None, bool]:
    """Classify a transaction by matching keywords against the description.

    Returns:
        (category, is_revenue) — category is None if unclassified.
        is_revenue is True for items like 決算お利息 that are income, not expenses.
    """
    if not description:
        return None, False

    rules = get_all_expense_rules()
    desc_upper = description.upper()

    for rule in rules:
        keyword = rule["keyword"]
        if keyword.upper() in desc_upper or keyword in description:
            cat = rule["category"]
            if cat == "_収入":
                return cat, True
            return cat, False

    return None, False


def classify_expense_batch(descriptions: list[str]) -> list[tuple[str | None, bool]]:
    """Classify multiple descriptions efficiently (single DB call)."""
    rules = get_all_expense_rules()
    results = []
    for desc in descriptions:
        if not desc:
            results.append((None, False))
            continue
        found = False
        desc_upper = desc.upper()
        for rule in rules:
            keyword = rule["keyword"]
            if keyword.upper() in desc_upper or keyword in desc:
                cat = rule["category"]
                if cat == "_収入":
                    results.append((cat, True))
                else:
                    results.append((cat, False))
                found = True
                break
        if not found:
            results.append((None, False))
    return results
