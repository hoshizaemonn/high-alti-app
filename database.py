"""SQLite database setup and CRUD operations for High-Alti PL Dashboard."""

import sqlite3
import os
from pathlib import Path

DB_DIR = Path(__file__).parent / "data"
DB_PATH = DB_DIR / "highalt.db"

STORES = ["東日本橋", "春日", "船橋", "巣鴨", "祖師ヶ谷大蔵", "下北沢", "中目黒"]
HQ_STORE = "本部（除外）"

# Thousand-digit → store mapping
THOUSAND_DIGIT_MAP = {
    1: "東日本橋",
    2: "春日",
    3: "船橋",
    4: "巣鴨",
    5: "東日本橋",  # fallback — overrides handle most 5xxx
    6: "祖師ヶ谷大蔵",
    7: "下北沢",
    8: "中目黒",
}

INITIAL_OVERRIDES = [
    (24, "船橋", 100),
    (25, "巣鴨", 100),
    (26, "中目黒", 100),
    (27, "祖師ヶ谷大蔵", 100),
    (28, "東日本橋", 100),
    (29, "下北沢", 100),
    (1027, "祖師ヶ谷大蔵", 100),
    (4013, "東日本橋", 100),
    (4015, "春日", 100),
    (4018, "春日", 100),
    (5007, "東日本橋", 100),
    (5009, "祖師ヶ谷大蔵", 100),
    # Dual assignment
    (4005, "春日", 60),
    (4005, "巣鴨", 40),
]

INITIAL_EXPENSE_RULES = [
    ("AMAZON", "消耗品費"),
    ("ＡＭＡＺＯＮ", "消耗品費"),
    ("プリントパック", "広告宣伝費"),
    ("印刷通販プリントパック", "広告宣伝費"),
    ("ラクスル", "広告宣伝費"),
    ("ダスキン", "委託料"),
    ("セコム", "委託料"),
    ("SMBC", "委託料"),
    ("ＳＭＢＣ", "委託料"),
    ("テレポ", "通信費"),
    ("BIZIMO", "通信費"),
    ("ＢＩＺＩＭＯ", "通信費"),
    ("ダイワショウケン", "賃借料"),
    ("ダイワシヨウケン", "賃借料"),
    ("ネットプロテクション", "支払手数料"),
    ("ネツトプロテクシヨンズ", "支払手数料"),
    ("振込手数料", "支払手数料"),
    ("決算お利息", "_収入"),
]

EXPENSE_CATEGORIES = [
    "消耗品費",
    "広告宣伝費",
    "委託料",
    "通信費",
    "賃借料",
    "支払手数料",
    "雑費",
    "その他",
]


def get_connection():
    DB_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    conn = get_connection()
    c = conn.cursor()

    # Store override table
    c.execute("""
        CREATE TABLE IF NOT EXISTS store_overrides (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            employee_id INTEGER NOT NULL,
            store_name TEXT NOT NULL,
            ratio INTEGER NOT NULL DEFAULT 100,
            UNIQUE(employee_id, store_name)
        )
    """)

    # Expense classification rules
    c.execute("""
        CREATE TABLE IF NOT EXISTS expense_rules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            keyword TEXT NOT NULL UNIQUE,
            category TEXT NOT NULL
        )
    """)

    # Payroll data (monthly)
    c.execute("""
        CREATE TABLE IF NOT EXISTS payroll_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            employee_id TEXT NOT NULL,
            employee_name TEXT,
            contract_type TEXT,
            store_name TEXT NOT NULL,
            ratio INTEGER NOT NULL DEFAULT 100,
            work_days_weekday REAL DEFAULT 0,
            work_days_holiday REAL DEFAULT 0,
            work_days_legal_holiday REAL DEFAULT 0,
            scheduled_hours REAL DEFAULT 0,
            overtime_hours REAL DEFAULT 0,
            base_salary REAL DEFAULT 0,
            position_allowance REAL DEFAULT 0,
            overtime_pay REAL DEFAULT 0,
            commute_taxable REAL DEFAULT 0,
            commute_nontax REAL DEFAULT 0,
            taxable_total REAL DEFAULT 0,
            gross_total REAL DEFAULT 0,
            health_insurance_co REAL DEFAULT 0,
            care_insurance_co REAL DEFAULT 0,
            pension_co REAL DEFAULT 0,
            child_contribution_co REAL DEFAULT 0,
            pension_fund_co REAL DEFAULT 0,
            employment_insurance_co REAL DEFAULT 0,
            workers_comp_co REAL DEFAULT 0,
            general_contribution_co REAL DEFAULT 0,
            UNIQUE(year, month, employee_id, store_name)
        )
    """)

    # Expense data (monthly)
    c.execute("""
        CREATE TABLE IF NOT EXISTS expense_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            day INTEGER NOT NULL,
            store_name TEXT NOT NULL DEFAULT '',
            description TEXT,
            amount REAL DEFAULT 0,
            deposit REAL DEFAULT 0,
            category TEXT,
            is_revenue INTEGER DEFAULT 0
        )
    """)

    # Revenue data (monthly, placeholder)
    c.execute("""
        CREATE TABLE IF NOT EXISTS revenue_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            store_name TEXT NOT NULL,
            category TEXT DEFAULT '売上',
            amount REAL DEFAULT 0,
            member_count INTEGER DEFAULT 0,
            note TEXT
        )
    """)

    # Member data from hacomono ML001 (v2 — extended fields)
    c.execute("""
        CREATE TABLE IF NOT EXISTS member_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            store_name TEXT NOT NULL,
            member_id TEXT,
            member_name TEXT,
            plan_name TEXT,
            join_date TEXT,
            tenure TEXT,
            is_active INTEGER NOT NULL DEFAULT 1,
            is_new INTEGER NOT NULL DEFAULT 0,
            had_trial INTEGER NOT NULL DEFAULT 0,
            plan_end_date TEXT,
            trial_date TEXT,
            first_trial_date TEXT,
            initial_plan TEXT
        )
    """)

    # Monthly summary data from hacomono MA002
    c.execute("""
        CREATE TABLE IF NOT EXISTS monthly_summary (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            store_name TEXT NOT NULL DEFAULT '',
            total_members INTEGER DEFAULT 0,
            plan_subscribers INTEGER DEFAULT 0,
            plan_subscribers_1st INTEGER DEFAULT 0,
            new_registrations INTEGER DEFAULT 0,
            new_plan_applications INTEGER DEFAULT 0,
            new_plan_signups INTEGER DEFAULT 0,
            plan_changes INTEGER DEFAULT 0,
            suspensions INTEGER DEFAULT 0,
            cancellations INTEGER DEFAULT 0,
            cancellation_rate TEXT DEFAULT ''
        )
    """)

    # Migration: add columns if they don't exist (for existing DBs)
    try:
        c.execute("SELECT is_active FROM member_data LIMIT 1")
    except sqlite3.OperationalError:
        c.execute("ALTER TABLE member_data ADD COLUMN is_active INTEGER NOT NULL DEFAULT 1")
        c.execute("ALTER TABLE member_data ADD COLUMN is_new INTEGER NOT NULL DEFAULT 0")
        c.execute("ALTER TABLE member_data ADD COLUMN had_trial INTEGER NOT NULL DEFAULT 0")
        c.execute("ALTER TABLE member_data ADD COLUMN plan_end_date TEXT")
        c.execute("ALTER TABLE member_data ADD COLUMN trial_date TEXT")
        c.execute("ALTER TABLE member_data ADD COLUMN first_trial_date TEXT")
        c.execute("ALTER TABLE member_data ADD COLUMN initial_plan TEXT")

    # Seed overrides if table is empty
    c.execute("SELECT COUNT(*) FROM store_overrides")
    if c.fetchone()[0] == 0:
        for emp_id, store, ratio in INITIAL_OVERRIDES:
            c.execute(
                "INSERT OR IGNORE INTO store_overrides (employee_id, store_name, ratio) VALUES (?, ?, ?)",
                (emp_id, store, ratio),
            )

    # Seed expense rules if table is empty
    c.execute("SELECT COUNT(*) FROM expense_rules")
    if c.fetchone()[0] == 0:
        for keyword, category in INITIAL_EXPENSE_RULES:
            c.execute(
                "INSERT OR IGNORE INTO expense_rules (keyword, category) VALUES (?, ?)",
                (keyword, category),
            )

    conn.commit()
    conn.close()


# ─── Store Override CRUD ─────────────────────────────────────────────

def get_all_overrides():
    conn = get_connection()
    rows = conn.execute(
        "SELECT id, employee_id, store_name, ratio FROM store_overrides ORDER BY employee_id, store_name"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def upsert_override(employee_id: int, store_name: str, ratio: int):
    conn = get_connection()
    conn.execute(
        """INSERT INTO store_overrides (employee_id, store_name, ratio)
           VALUES (?, ?, ?)
           ON CONFLICT(employee_id, store_name)
           DO UPDATE SET ratio = excluded.ratio""",
        (employee_id, store_name, ratio),
    )
    conn.commit()
    conn.close()


def delete_override(override_id: int):
    conn = get_connection()
    conn.execute("DELETE FROM store_overrides WHERE id = ?", (override_id,))
    conn.commit()
    conn.close()


def get_overrides_for_employee(employee_id: int):
    conn = get_connection()
    rows = conn.execute(
        "SELECT store_name, ratio FROM store_overrides WHERE employee_id = ? ORDER BY ratio DESC",
        (employee_id,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ─── Expense Rules CRUD ─────────────────────────────────────────────

def get_all_expense_rules():
    conn = get_connection()
    rows = conn.execute(
        "SELECT id, keyword, category FROM expense_rules ORDER BY keyword"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def upsert_expense_rule(keyword: str, category: str):
    conn = get_connection()
    conn.execute(
        """INSERT INTO expense_rules (keyword, category)
           VALUES (?, ?)
           ON CONFLICT(keyword)
           DO UPDATE SET category = excluded.category""",
        (keyword, category),
    )
    conn.commit()
    conn.close()


def delete_expense_rule(rule_id: int):
    conn = get_connection()
    conn.execute("DELETE FROM expense_rules WHERE id = ?", (rule_id,))
    conn.commit()
    conn.close()


# ─── Payroll Data ────────────────────────────────────────────────────

def save_payroll_data(records: list[dict]):
    """Save a list of payroll record dicts. Replaces existing data for the same year/month."""
    if not records:
        return
    conn = get_connection()
    year = records[0]["year"]
    month = records[0]["month"]
    conn.execute("DELETE FROM payroll_data WHERE year = ? AND month = ?", (year, month))
    for r in records:
        conn.execute(
            """INSERT INTO payroll_data (
                year, month, employee_id, employee_name, contract_type,
                store_name, ratio,
                work_days_weekday, work_days_holiday, work_days_legal_holiday,
                scheduled_hours, overtime_hours,
                base_salary, position_allowance, overtime_pay,
                commute_taxable, commute_nontax,
                taxable_total, gross_total,
                health_insurance_co, care_insurance_co, pension_co,
                child_contribution_co, pension_fund_co,
                employment_insurance_co, workers_comp_co, general_contribution_co
            ) VALUES (
                :year, :month, :employee_id, :employee_name, :contract_type,
                :store_name, :ratio,
                :work_days_weekday, :work_days_holiday, :work_days_legal_holiday,
                :scheduled_hours, :overtime_hours,
                :base_salary, :position_allowance, :overtime_pay,
                :commute_taxable, :commute_nontax,
                :taxable_total, :gross_total,
                :health_insurance_co, :care_insurance_co, :pension_co,
                :child_contribution_co, :pension_fund_co,
                :employment_insurance_co, :workers_comp_co, :general_contribution_co
            )""",
            r,
        )
    conn.commit()
    conn.close()


def get_payroll_data(year: int, month: int = None, store: str = None, include_hq: bool = False):
    conn = get_connection()
    query = "SELECT * FROM payroll_data WHERE year = ?"
    params = [year]
    if month is not None:
        query += " AND month = ?"
        params.append(month)
    if store is not None and store != "全体":
        query += " AND store_name = ?"
        params.append(store)
    if not include_hq:
        query += " AND store_name != ?"
        params.append(HQ_STORE)
    query += " ORDER BY store_name, employee_id"
    rows = conn.execute(query, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_payroll_months(year: int = None):
    conn = get_connection()
    if year:
        rows = conn.execute(
            "SELECT DISTINCT year, month FROM payroll_data WHERE year = ? ORDER BY month",
            (year,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT DISTINCT year, month FROM payroll_data ORDER BY year, month"
        ).fetchall()
    conn.close()
    return [(r["year"], r["month"]) for r in rows]


# ─── Expense Data ────────────────────────────────────────────────────

def save_expense_data(records: list[dict]):
    if not records:
        return
    conn = get_connection()
    year = records[0]["year"]
    month = records[0]["month"]
    store_name = records[0].get("store_name", "")
    conn.execute("DELETE FROM expense_data WHERE year = ? AND month = ? AND store_name = ?", (year, month, store_name))
    for r in records:
        conn.execute(
            """INSERT INTO expense_data (year, month, day, store_name, description, amount, deposit, category, is_revenue)
               VALUES (:year, :month, :day, :store_name, :description, :amount, :deposit, :category, :is_revenue)""",
            r,
        )
    conn.commit()
    conn.close()


def get_expense_data(year: int, month: int = None, store: str = None):
    conn = get_connection()
    query = "SELECT * FROM expense_data WHERE year = ?"
    params = [year]
    if month is not None:
        query += " AND month = ?"
        params.append(month)
    if store is not None and store != "全体":
        query += " AND store_name = ?"
        params.append(store)
    query += " ORDER BY month, day"
    rows = conn.execute(query, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_expense_months(year: int = None):
    conn = get_connection()
    if year:
        rows = conn.execute(
            "SELECT DISTINCT year, month FROM expense_data WHERE year = ? ORDER BY month",
            (year,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT DISTINCT year, month FROM expense_data ORDER BY year, month"
        ).fetchall()
    conn.close()
    return [(r["year"], r["month"]) for r in rows]


# ─── Revenue Data ────────────────────────────────────────────────────

def save_revenue_data(records: list[dict]):
    if not records:
        return
    conn = get_connection()
    year = records[0]["year"]
    month = records[0]["month"]
    store_name = records[0].get("store_name", "")
    conn.execute("DELETE FROM revenue_data WHERE year = ? AND month = ? AND store_name = ?", (year, month, store_name))
    for r in records:
        conn.execute(
            """INSERT INTO revenue_data (year, month, store_name, category, amount, member_count, note)
               VALUES (:year, :month, :store_name, :category, :amount, :member_count, :note)""",
            r,
        )
    conn.commit()
    conn.close()


def get_revenue_data(year: int, month: int = None, store: str = None):
    conn = get_connection()
    query = "SELECT * FROM revenue_data WHERE year = ?"
    params = [year]
    if month is not None:
        query += " AND month = ?"
        params.append(month)
    if store is not None and store != "全体":
        query += " AND store_name = ?"
        params.append(store)
    query += " ORDER BY month, store_name"
    rows = conn.execute(query, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_revenue_months(year: int = None):
    conn = get_connection()
    if year:
        rows = conn.execute(
            "SELECT DISTINCT year, month FROM revenue_data WHERE year = ? ORDER BY month",
            (year,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT DISTINCT year, month FROM revenue_data ORDER BY year, month"
        ).fetchall()
    conn.close()
    return [(r["year"], r["month"]) for r in rows]


# ─── Member Data ─────────────────────────────────────────────────────

def save_member_data(records: list[dict]):
    """Save member data from hacomono ML001 (v2). Replaces existing for same year/month/store."""
    if not records:
        return
    conn = get_connection()
    year = records[0]["year"]
    month = records[0]["month"]
    store_name = records[0].get("store_name", "")
    # Delete by year/month/store so multi-store uploads don't clobber each other
    conn.execute("DELETE FROM member_data WHERE year = ? AND month = ? AND store_name = ?", (year, month, store_name))
    for r in records:
        conn.execute(
            """INSERT INTO member_data (
                year, month, store_name, member_id, member_name,
                plan_name, join_date, tenure,
                is_active, is_new, had_trial,
                plan_end_date, trial_date, first_trial_date, initial_plan
            ) VALUES (
                :year, :month, :store_name, :member_id, :member_name,
                :plan_name, :join_date, :tenure,
                :is_active, :is_new, :had_trial,
                :plan_end_date, :trial_date, :first_trial_date, :initial_plan
            )""",
            r,
        )
    conn.commit()
    conn.close()


def get_member_data(year: int, month: int = None, store: str = None):
    conn = get_connection()
    query = "SELECT * FROM member_data WHERE year = ?"
    params = [year]
    if month is not None:
        query += " AND month = ?"
        params.append(month)
    if store is not None and store != "全体":
        query += " AND store_name = ?"
        params.append(store)
    query += " ORDER BY store_name, plan_name, member_id"
    rows = conn.execute(query, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_member_months(year: int = None):
    conn = get_connection()
    if year:
        rows = conn.execute(
            "SELECT DISTINCT year, month FROM member_data WHERE year = ? ORDER BY month",
            (year,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT DISTINCT year, month FROM member_data ORDER BY year, month"
        ).fetchall()
    conn.close()
    return [(r["year"], r["month"]) for r in rows]


def get_monthly_summary_months(year: int = None):
    conn = get_connection()
    if year:
        rows = conn.execute(
            "SELECT DISTINCT year, month FROM monthly_summary WHERE year = ? ORDER BY month",
            (year,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT DISTINCT year, month FROM monthly_summary ORDER BY year, month"
        ).fetchall()
    conn.close()
    return [(r["year"], r["month"]) for r in rows]


# ─── Monthly Summary Data (MA002) ──────────────────────────────────

def save_monthly_summary(records: list[dict]):
    """Save monthly summary records from hacomono MA002. Replaces existing for same year/month/store."""
    if not records:
        return
    conn = get_connection()
    year = records[0]["year"]
    month = records[0]["month"]
    store_name = records[0].get("store_name", "")
    conn.execute(
        "DELETE FROM monthly_summary WHERE year = ? AND month = ? AND store_name = ?",
        (year, month, store_name),
    )
    for r in records:
        conn.execute(
            """INSERT INTO monthly_summary (
                year, month, store_name, total_members, plan_subscribers,
                plan_subscribers_1st, new_registrations, new_plan_applications,
                new_plan_signups, plan_changes, suspensions, cancellations, cancellation_rate
            ) VALUES (
                :year, :month, :store_name, :total_members, :plan_subscribers,
                :plan_subscribers_1st, :new_registrations, :new_plan_applications,
                :new_plan_signups, :plan_changes, :suspensions, :cancellations, :cancellation_rate
            )""",
            r,
        )
    conn.commit()
    conn.close()


def get_monthly_summary(year: int, month: int = None, store: str = None):
    conn = get_connection()
    query = "SELECT * FROM monthly_summary WHERE year = ?"
    params = [year]
    if month is not None:
        query += " AND month = ?"
        params.append(month)
    if store is not None and store != "全体":
        query += " AND store_name = ?"
        params.append(store)
    query += " ORDER BY month, store_name"
    rows = conn.execute(query, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_member_summary_stats(year: int, month: int, store: str = None) -> dict:
    """Compute summary stats from stored member records.

    Returns dict with: total, active,休会, new, trial, plan_breakdown, churn
    """
    records = get_member_data(year, month, store)
    if not records:
        return {
            "total": 0, "active": 0, "suspended": 0, "new": 0,
            "trial": 0, "plan_breakdown": {}, "by_store": {},
        }

    import pandas as pd
    df = pd.DataFrame(records)

    total = len(df)
    active = int(df["is_active"].sum())
    suspended = total - active
    new_count = int(df["is_new"].sum())
    trial_count = int(df["had_trial"].sum())

    plan_breakdown = df.groupby("plan_name").size().sort_values(ascending=False).to_dict()
    by_store = df.groupby("store_name").size().to_dict()

    # Active breakdown by plan
    active_df = df[df["is_active"] == 1]
    active_plan_breakdown = active_df.groupby("plan_name").size().sort_values(ascending=False).to_dict() if not active_df.empty else {}

    return {
        "total": total,
        "active": active,
        "suspended": suspended,
        "new": new_count,
        "trial": trial_count,
        "plan_breakdown": plan_breakdown,
        "active_plan_breakdown": active_plan_breakdown,
        "by_store": by_store,
    }


# ─── Data availability ───────────────────────────────────────────────

def get_available_years():
    conn = get_connection()
    rows = conn.execute("""
        SELECT DISTINCT year FROM (
            SELECT year FROM payroll_data
            UNION SELECT year FROM expense_data
            UNION SELECT year FROM revenue_data
            UNION SELECT year FROM member_data
            UNION SELECT year FROM monthly_summary
        ) ORDER BY year
    """).fetchall()
    conn.close()
    return [r["year"] for r in rows]


def get_available_months(year: int):
    conn = get_connection()
    rows = conn.execute("""
        SELECT DISTINCT month FROM (
            SELECT month FROM payroll_data WHERE year = ?
            UNION SELECT month FROM expense_data WHERE year = ?
            UNION SELECT month FROM revenue_data WHERE year = ?
            UNION SELECT month FROM member_data WHERE year = ?
            UNION SELECT month FROM monthly_summary WHERE year = ?
        ) ORDER BY month
    """, (year, year, year, year, year)).fetchall()
    conn.close()
    return [r["month"] for r in rows]
