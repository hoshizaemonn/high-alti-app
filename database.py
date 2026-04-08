"""Dual-mode database (PostgreSQL / SQLite) for High-Alti PL Dashboard.

When the environment variable DATABASE_URL is set, connects to PostgreSQL
(Supabase / Render).  Otherwise falls back to local SQLite for development.
"""

import sqlite3
import os
import hashlib
from pathlib import Path

# Try to import psycopg2; not required for local-only SQLite usage
try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False

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


# ─── Helpers ──────────────────────────────────────────────────────────

def _is_pg() -> bool:
    """Return True when PostgreSQL mode is active."""
    return bool(os.environ.get("DATABASE_URL")) and HAS_PSYCOPG2


def _ph(sql: str) -> str:
    """Convert SQLite-style `?` placeholders to `%s` for PostgreSQL."""
    if _is_pg():
        return sql.replace("?", "%s")
    return sql


def _param(n: int) -> str:
    """Return a positional placeholder string.

    Both psycopg2 and sqlite3 use positional markers (%s and ? respectively).
    The `n` argument is kept for readability but not used in the output.
    """
    return "%s" if _is_pg() else "?"


def _named_to_positional(sql: str, data: dict) -> tuple[str, tuple]:
    """Convert `:name` named placeholders + dict → positional SQL + tuple.

    SQLite natively supports `:name` with a dict, but psycopg2 uses
    `%(name)s`.  To keep a single code path we convert to positional.
    """
    import re
    params = []
    names = []

    def _replacer(m):
        name = m.group(1)
        names.append(name)
        if _is_pg():
            return "%s"
        return ":" + name  # keep as-is for SQLite

    converted = re.sub(r":(\w+)", _replacer, sql)
    if _is_pg():
        params = tuple(data[n] for n in names)
        return converted, params
    else:
        return sql, data  # SQLite uses the original dict directly


def get_connection():
    """Return a DB connection (PostgreSQL or SQLite)."""
    if _is_pg():
        conn = psycopg2.connect(os.environ["DATABASE_URL"], cursor_factory=RealDictCursor)
        return conn
    else:
        DB_DIR.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(DB_PATH))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        return conn


def _execute(conn, sql, params=None):
    """Execute SQL via cursor (works for both PG and SQLite).

    For SQLite, conn.execute() is fine.
    For PostgreSQL, we must use cursor.execute().
    Returns the cursor.
    """
    if _is_pg():
        cur = conn.cursor()
        cur.execute(sql, params)
        return cur
    else:
        if params is not None:
            return conn.execute(sql, params)
        return conn.execute(sql)


def _fetchall(conn, sql, params=None):
    """Execute and fetchall, returning list of dict-like rows."""
    cur = _execute(conn, sql, params)
    rows = cur.fetchall()
    if _is_pg():
        cur.close()
    return rows


def _fetchone(conn, sql, params=None):
    """Execute and fetchone, returning a single dict-like row or None."""
    cur = _execute(conn, sql, params)
    row = cur.fetchone()
    if _is_pg():
        cur.close()
    return row


def _row_to_dict(row):
    """Convert a row to a plain dict (works for both sqlite3.Row and RealDictRow)."""
    if row is None:
        return None
    if _is_pg():
        return dict(row)  # RealDictRow is already dict-like
    return dict(row)


# ─── Schema / Init ────────────────────────────────────────────────────

def init_db():
    conn = get_connection()
    if _is_pg():
        conn.autocommit = True

    auto_id = "SERIAL PRIMARY KEY" if _is_pg() else "INTEGER PRIMARY KEY AUTOINCREMENT"

    # Store override table
    _execute(conn, f"""
        CREATE TABLE IF NOT EXISTS store_overrides (
            id {auto_id},
            employee_id INTEGER NOT NULL,
            store_name TEXT NOT NULL,
            ratio INTEGER NOT NULL DEFAULT 100,
            UNIQUE(employee_id, store_name)
        )
    """)

    # Expense classification rules
    _execute(conn, f"""
        CREATE TABLE IF NOT EXISTS expense_rules (
            id {auto_id},
            keyword TEXT NOT NULL UNIQUE,
            category TEXT NOT NULL
        )
    """)

    # Payroll data (monthly)
    _execute(conn, f"""
        CREATE TABLE IF NOT EXISTS payroll_data (
            id {auto_id},
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
    _execute(conn, f"""
        CREATE TABLE IF NOT EXISTS expense_data (
            id {auto_id},
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            day INTEGER NOT NULL,
            store_name TEXT NOT NULL DEFAULT '',
            description TEXT,
            amount REAL DEFAULT 0,
            deposit REAL DEFAULT 0,
            category TEXT,
            is_revenue INTEGER DEFAULT 0,
            breakdown TEXT DEFAULT ''
        )
    """)

    # Revenue data (monthly, placeholder)
    _execute(conn, f"""
        CREATE TABLE IF NOT EXISTS revenue_data (
            id {auto_id},
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
    _execute(conn, f"""
        CREATE TABLE IF NOT EXISTS member_data (
            id {auto_id},
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
    _execute(conn, f"""
        CREATE TABLE IF NOT EXISTS monthly_summary (
            id {auto_id},
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

    # Sales detail data from hacomono PL001
    _execute(conn, f"""
        CREATE TABLE IF NOT EXISTS sales_detail (
            id {auto_id},
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            store_name TEXT NOT NULL,
            sale_id TEXT,
            sale_date TEXT,
            payment_method TEXT,
            description TEXT,
            category TEXT,
            amount INTEGER DEFAULT 0,
            tax INTEGER DEFAULT 0,
            discount INTEGER DEFAULT 0
        )
    """)

    # Breakdown rules — 摘要 + 金額パターン→内訳 の自動ルール
    _execute(conn, f"""
        CREATE TABLE IF NOT EXISTS breakdown_rules (
            id {auto_id},
            description_pattern TEXT NOT NULL,
            amount INTEGER,
            breakdown TEXT NOT NULL,
            UNIQUE(description_pattern, amount)
        )
    """)

    # Square sales summary
    _execute(conn, f"""
        CREATE TABLE IF NOT EXISTS square_sales (
            id {auto_id},
            store_name TEXT NOT NULL,
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            gross_sales INTEGER DEFAULT 0,
            net_sales INTEGER DEFAULT 0,
            fees INTEGER DEFAULT 0,
            transaction_count INTEGER DEFAULT 0,
            UNIQUE(store_name, year, month)
        )
    """)

    # Budget data (予算)
    _execute(conn, f"""
        CREATE TABLE IF NOT EXISTS budget_data (
            id {auto_id},
            store_name TEXT NOT NULL,
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            category TEXT NOT NULL,
            amount INTEGER DEFAULT 0,
            UNIQUE(store_name, year, month, category)
        )
    """)

    # Amazon orders table
    _execute(conn, f"""
        CREATE TABLE IF NOT EXISTS amazon_orders (
            id {auto_id},
            order_date TEXT,
            order_id TEXT,
            store_name TEXT,
            product_name TEXT,
            short_name TEXT,
            amount INTEGER,
            order_total INTEGER,
            payment_date TEXT,
            delivery_address TEXT,
            UNIQUE(order_id, product_name)
        )
    """)

    # Users table
    _execute(conn, f"""
        CREATE TABLE IF NOT EXISTS users (
            id {auto_id},
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT 'store_manager',
            store_name TEXT,
            display_name TEXT
        )
    """)

    # Migration: add columns if they don't exist
    if _is_pg():
        # PostgreSQL: use ADD COLUMN IF NOT EXISTS
        for col, coldef in [
            ("is_active", "INTEGER NOT NULL DEFAULT 1"),
            ("is_new", "INTEGER NOT NULL DEFAULT 0"),
            ("had_trial", "INTEGER NOT NULL DEFAULT 0"),
            ("plan_end_date", "TEXT"),
            ("trial_date", "TEXT"),
            ("first_trial_date", "TEXT"),
            ("initial_plan", "TEXT"),
        ]:
            try:
                _execute(conn, f"ALTER TABLE member_data ADD COLUMN {col} {coldef}")
            except Exception:
                conn.rollback()  # PG requires rollback after failed statement

        # breakdown column on expense_data
        try:
            _execute(conn, "ALTER TABLE expense_data ADD COLUMN breakdown TEXT DEFAULT ''")
        except Exception:
            conn.rollback()
    else:
        # SQLite migration
        try:
            _execute(conn, "SELECT is_active FROM member_data LIMIT 1")
        except sqlite3.OperationalError:
            _execute(conn, "ALTER TABLE member_data ADD COLUMN is_active INTEGER NOT NULL DEFAULT 1")
            _execute(conn, "ALTER TABLE member_data ADD COLUMN is_new INTEGER NOT NULL DEFAULT 0")
            _execute(conn, "ALTER TABLE member_data ADD COLUMN had_trial INTEGER NOT NULL DEFAULT 0")
            _execute(conn, "ALTER TABLE member_data ADD COLUMN plan_end_date TEXT")
            _execute(conn, "ALTER TABLE member_data ADD COLUMN trial_date TEXT")
            _execute(conn, "ALTER TABLE member_data ADD COLUMN first_trial_date TEXT")
            _execute(conn, "ALTER TABLE member_data ADD COLUMN initial_plan TEXT")

        try:
            _execute(conn, "SELECT breakdown FROM expense_data LIMIT 1")
        except sqlite3.OperationalError:
            _execute(conn, "ALTER TABLE expense_data ADD COLUMN breakdown TEXT DEFAULT ''")

    conn.commit()

    # Seed overrides if table is empty
    row = _fetchone(conn, "SELECT COUNT(*) as cnt FROM store_overrides")
    cnt = row["cnt"] if _is_pg() else row[0] if not isinstance(row, dict) else row["cnt"]
    if cnt == 0:
        insert_sql = _ph(
            "INSERT INTO store_overrides (employee_id, store_name, ratio) VALUES (%s, %s, %s)"
        ) if _is_pg() else "INSERT OR IGNORE INTO store_overrides (employee_id, store_name, ratio) VALUES (?, ?, ?)"
        for emp_id, store, ratio in INITIAL_OVERRIDES:
            if _is_pg():
                try:
                    _execute(conn, insert_sql, (emp_id, store, ratio))
                except Exception:
                    conn.rollback()
            else:
                _execute(conn, insert_sql, (emp_id, store, ratio))

    # Seed expense rules if table is empty
    row = _fetchone(conn, "SELECT COUNT(*) as cnt FROM expense_rules")
    cnt = row["cnt"] if _is_pg() else row[0] if not isinstance(row, dict) else row["cnt"]
    if cnt == 0:
        insert_sql = _ph(
            "INSERT INTO expense_rules (keyword, category) VALUES (%s, %s)"
        ) if _is_pg() else "INSERT OR IGNORE INTO expense_rules (keyword, category) VALUES (?, ?)"
        for keyword, category in INITIAL_EXPENSE_RULES:
            if _is_pg():
                try:
                    _execute(conn, insert_sql, (keyword, category))
                except Exception:
                    conn.rollback()
            else:
                _execute(conn, insert_sql, (keyword, category))

    # Seed default admin user if users table is empty
    row = _fetchone(conn, "SELECT COUNT(*) as cnt FROM users")
    cnt = row["cnt"] if _is_pg() else row[0] if not isinstance(row, dict) else row["cnt"]
    if cnt == 0:
        admin_hash = hashlib.sha256("admin123".encode()).hexdigest()
        _execute(conn, _ph(
            "INSERT INTO users (username, password_hash, role, store_name, display_name) VALUES (%s, %s, %s, %s, %s)"
        ) if _is_pg() else
            "INSERT INTO users (username, password_hash, role, store_name, display_name) VALUES (?, ?, ?, ?, ?)",
            ("admin", admin_hash, "admin", "", "管理者"),
        )

    conn.commit()
    conn.close()


# ─── Store Override CRUD ─────────────────────────────────────────────

def get_all_overrides():
    conn = get_connection()
    rows = _fetchall(conn, "SELECT id, employee_id, store_name, ratio FROM store_overrides ORDER BY employee_id, store_name")
    conn.close()
    return [dict(r) for r in rows]


def upsert_override(employee_id: int, store_name: str, ratio: int):
    conn = get_connection()
    if _is_pg():
        _execute(conn,
            """INSERT INTO store_overrides (employee_id, store_name, ratio)
               VALUES (%s, %s, %s)
               ON CONFLICT(employee_id, store_name)
               DO UPDATE SET ratio = EXCLUDED.ratio""",
            (employee_id, store_name, ratio),
        )
    else:
        _execute(conn,
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
    _execute(conn, _ph("DELETE FROM store_overrides WHERE id = ?"), (override_id,))
    conn.commit()
    conn.close()


def get_overrides_for_employee(employee_id: int):
    conn = get_connection()
    rows = _fetchall(conn,
        _ph("SELECT store_name, ratio FROM store_overrides WHERE employee_id = ? ORDER BY ratio DESC"),
        (employee_id,),
    )
    conn.close()
    return [dict(r) for r in rows]


# ─── Expense Rules CRUD ─────────────────────────────────────────────

def get_all_expense_rules():
    conn = get_connection()
    rows = _fetchall(conn, "SELECT id, keyword, category FROM expense_rules ORDER BY keyword")
    conn.close()
    return [dict(r) for r in rows]


def upsert_expense_rule(keyword: str, category: str):
    conn = get_connection()
    if _is_pg():
        _execute(conn,
            """INSERT INTO expense_rules (keyword, category)
               VALUES (%s, %s)
               ON CONFLICT(keyword)
               DO UPDATE SET category = EXCLUDED.category""",
            (keyword, category),
        )
    else:
        _execute(conn,
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
    _execute(conn, _ph("DELETE FROM expense_rules WHERE id = ?"), (rule_id,))
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
    _execute(conn, _ph("DELETE FROM payroll_data WHERE year = ? AND month = ?"), (year, month))

    insert_sql = """INSERT INTO payroll_data (
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
            )"""

    for r in records:
        converted_sql, params = _named_to_positional(insert_sql, r)
        _execute(conn, converted_sql, params)
    conn.commit()
    conn.close()


def get_payroll_data(year: int, month: int = None, store: str = None, include_hq: bool = False):
    conn = get_connection()
    query = "SELECT * FROM payroll_data WHERE year = " + _param(1)
    params = [year]
    if month is not None:
        query += " AND month = " + _param(len(params) + 1)
        params.append(month)
    if store is not None and store != "全体":
        query += " AND store_name = " + _param(len(params) + 1)
        params.append(store)
    if not include_hq:
        query += " AND store_name != " + _param(len(params) + 1)
        params.append(HQ_STORE)
    query += " ORDER BY store_name, employee_id"
    rows = _fetchall(conn, query, tuple(params))
    conn.close()
    return [dict(r) for r in rows]


def get_payroll_months(year: int = None):
    conn = get_connection()
    if year:
        rows = _fetchall(conn,
            _ph("SELECT DISTINCT year, month FROM payroll_data WHERE year = ? ORDER BY month"),
            (year,),
        )
    else:
        rows = _fetchall(conn,
            "SELECT DISTINCT year, month FROM payroll_data ORDER BY year, month"
        )
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
    _execute(conn, _ph("DELETE FROM expense_data WHERE year = ? AND month = ? AND store_name = ?"), (year, month, store_name))

    insert_sql = """INSERT INTO expense_data (year, month, day, store_name, description, amount, deposit, category, is_revenue, breakdown)
               VALUES (:year, :month, :day, :store_name, :description, :amount, :deposit, :category, :is_revenue, :breakdown)"""

    for r in records:
        r.setdefault("breakdown", "")
        converted_sql, params = _named_to_positional(insert_sql, r)
        _execute(conn, converted_sql, params)
    conn.commit()
    conn.close()


def get_expense_data(year: int, month: int = None, store: str = None):
    conn = get_connection()
    query = "SELECT * FROM expense_data WHERE year = " + _param(1)
    params = [year]
    if month is not None:
        query += " AND month = " + _param(len(params) + 1)
        params.append(month)
    if store is not None and store != "全体":
        query += " AND store_name = " + _param(len(params) + 1)
        params.append(store)
    query += " ORDER BY month, day"
    rows = _fetchall(conn, query, tuple(params))
    conn.close()
    return [dict(r) for r in rows]


def get_expense_months(year: int = None):
    conn = get_connection()
    if year:
        rows = _fetchall(conn,
            _ph("SELECT DISTINCT year, month FROM expense_data WHERE year = ? ORDER BY month"),
            (year,),
        )
    else:
        rows = _fetchall(conn,
            "SELECT DISTINCT year, month FROM expense_data ORDER BY year, month"
        )
    conn.close()
    return [(r["year"], r["month"]) for r in rows]


# ─── Breakdown Rules ───────────────────────────────────────────────

def upsert_breakdown_rule(description: str, amount: int, breakdown: str):
    """Save a manual breakdown entry as a rule for future auto-fill."""
    conn = get_connection()
    if _is_pg():
        _execute(conn,
            """INSERT INTO breakdown_rules (description_pattern, amount, breakdown)
               VALUES (%s, %s, %s)
               ON CONFLICT(description_pattern, amount)
               DO UPDATE SET breakdown = EXCLUDED.breakdown""",
            (description.strip(), int(amount), breakdown),
        )
    else:
        _execute(conn,
            """INSERT INTO breakdown_rules (description_pattern, amount, breakdown)
               VALUES (?, ?, ?)
               ON CONFLICT(description_pattern, amount)
               DO UPDATE SET breakdown = excluded.breakdown""",
            (description.strip(), int(amount), breakdown),
        )
    conn.commit()
    conn.close()


def find_breakdown_rule(description: str, amount: int) -> str:
    """Find a matching breakdown rule for given description and amount."""
    conn = get_connection()
    row = _fetchone(conn,
        _ph("SELECT breakdown FROM breakdown_rules WHERE description_pattern = ? AND amount = ?"),
        (description.strip(), int(amount)),
    )
    conn.close()
    return row["breakdown"] if row else ""


def apply_breakdown_rules_to_expense_data():
    """Apply all saved breakdown rules to expense_data for rows with empty breakdown."""
    conn = get_connection()
    rules = _fetchall(conn, "SELECT description_pattern, amount, breakdown FROM breakdown_rules")
    updated = 0
    for r in rules:
        cur = _execute(conn,
            _ph("""UPDATE expense_data SET breakdown = ?
               WHERE (breakdown = '' OR breakdown IS NULL)
               AND description = ? AND amount = ?"""),
            (r["breakdown"], r["description_pattern"], r["amount"]),
        )
        updated += cur.rowcount
    conn.commit()
    conn.close()
    return updated


# ─── Square Sales ───────────────────────────────────────────────────

def save_square_sales(records: list[dict]) -> int:
    if not records:
        return 0
    conn = get_connection()

    insert_sql = """INSERT INTO square_sales (store_name, year, month, gross_sales, net_sales, fees, transaction_count)
               VALUES (:store_name, :year, :month, :gross_sales, :net_sales, :fees, :transaction_count)"""

    if _is_pg():
        for r in records:
            converted_sql, params = _named_to_positional(insert_sql, r)
            # Append ON CONFLICT for PostgreSQL
            converted_sql += """
               ON CONFLICT(store_name, year, month)
               DO UPDATE SET gross_sales=EXCLUDED.gross_sales, net_sales=EXCLUDED.net_sales,
                             fees=EXCLUDED.fees, transaction_count=EXCLUDED.transaction_count"""
            _execute(conn, converted_sql, params)
    else:
        for r in records:
            _execute(conn,
                """INSERT INTO square_sales (store_name, year, month, gross_sales, net_sales, fees, transaction_count)
                   VALUES (:store_name, :year, :month, :gross_sales, :net_sales, :fees, :transaction_count)
                   ON CONFLICT(store_name, year, month)
                   DO UPDATE SET gross_sales=excluded.gross_sales, net_sales=excluded.net_sales,
                                 fees=excluded.fees, transaction_count=excluded.transaction_count""",
                r,
            )
    conn.commit()
    conn.close()
    return len(records)


def get_square_sales(store: str = None, year: int = None, month: int = None):
    conn = get_connection()
    query = "SELECT * FROM square_sales WHERE 1=1"
    params = []
    if store is not None and store != "全体":
        query += " AND store_name = " + _param(len(params) + 1)
        params.append(store)
    if year is not None:
        query += " AND year = " + _param(len(params) + 1)
        params.append(year)
    if month is not None:
        query += " AND month = " + _param(len(params) + 1)
        params.append(month)
    rows = _fetchall(conn, query, tuple(params))
    conn.close()
    return [dict(r) for r in rows]


# ─── Budget Data ────────────────────────────────────────────────────

# Budget items (科目) in display order
BUDGET_ITEMS = [
    "パーソナル・物販・その他収入",
    "月会費収入",
    "サービス収入",
    "自販機手数料収入",
    "仕入高",
    "広告宣伝費",
    "正社員・契約社員給与",
    "賞与",
    "通勤手当",
    "法定福利費",
    "福利厚生費",
    "修繕費",
    "減価償却費",
    "賃借料",
    "消耗品費",
    "備品費",
    "電気料",
    "上下水道料",
    "通信費",
    "研修費",
    "支払手数料",
    "リース料",
    "委託料",
    "保険料",
    "接待交際費",
    "開発費償却",
    "租税公課",
]


def save_budget_data(records: list[dict]) -> int:
    """Save budget records. Replaces existing records for same store/year/month/category."""
    if not records:
        return 0
    conn = get_connection()

    insert_sql = """INSERT INTO budget_data (store_name, year, month, category, amount)
               VALUES (:store_name, :year, :month, :category, :amount)"""

    if _is_pg():
        for r in records:
            converted_sql, params = _named_to_positional(insert_sql, r)
            converted_sql += """
               ON CONFLICT(store_name, year, month, category)
               DO UPDATE SET amount = EXCLUDED.amount"""
            _execute(conn, converted_sql, params)
    else:
        for r in records:
            _execute(conn,
                """INSERT INTO budget_data (store_name, year, month, category, amount)
                   VALUES (:store_name, :year, :month, :category, :amount)
                   ON CONFLICT(store_name, year, month, category)
                   DO UPDATE SET amount = excluded.amount""",
                r,
            )
    conn.commit()
    conn.close()
    return len(records)


def get_budget_data(store: str = None, year: int = None, month: int = None):
    conn = get_connection()
    query = "SELECT * FROM budget_data WHERE 1=1"
    params = []
    if store is not None and store != "全体":
        query += " AND store_name = " + _param(len(params) + 1)
        params.append(store)
    if year is not None:
        query += " AND year = " + _param(len(params) + 1)
        params.append(year)
    if month is not None:
        query += " AND month = " + _param(len(params) + 1)
        params.append(month)
    query += " ORDER BY year, month, category"
    rows = _fetchall(conn, query, tuple(params))
    conn.close()
    return [dict(r) for r in rows]


def check_budget_exists(store: str, fiscal_year: int) -> int:
    """Check if budget data exists for a given store + fiscal year (Oct-Sep)."""
    conn = get_connection()
    # Fiscal year 2026 = 2025/10 to 2026/9
    row = _fetchone(conn,
        _ph("""SELECT COUNT(*) as cnt FROM budget_data WHERE store_name = ?
           AND ((year = ? AND month >= 10) OR (year = ? AND month <= 9))"""),
        (store, fiscal_year - 1, fiscal_year),
    )
    conn.close()
    if _is_pg():
        return row["cnt"]
    return row[0] if not isinstance(row, dict) else row["cnt"]


# ─── Data existence checks ──────────────────────────────────────────

def check_payroll_exists(year: int, month: int) -> int:
    conn = get_connection()
    row = _fetchone(conn,
        _ph("SELECT COUNT(*) as cnt FROM payroll_data WHERE year = ? AND month = ?"),
        (year, month),
    )
    conn.close()
    return row["cnt"]


def check_expense_exists(year: int, month: int, store: str) -> int:
    conn = get_connection()
    row = _fetchone(conn,
        _ph("SELECT COUNT(*) as cnt FROM expense_data WHERE year = ? AND month = ? AND store_name = ?"),
        (year, month, store),
    )
    conn.close()
    return row["cnt"]


def check_member_exists(store: str) -> int:
    conn = get_connection()
    row = _fetchone(conn,
        _ph("SELECT COUNT(*) as cnt FROM member_data WHERE store_name = ?"),
        (store,),
    )
    conn.close()
    return row["cnt"]


def check_sales_detail_exists(year: int, month: int, store: str) -> int:
    conn = get_connection()
    row = _fetchone(conn,
        _ph("SELECT COUNT(*) as cnt FROM sales_detail WHERE year = ? AND month = ? AND store_name = ?"),
        (year, month, store),
    )
    conn.close()
    return row["cnt"]


def check_monthly_summary_exists(year: int, month: int, store: str) -> int:
    conn = get_connection()
    row = _fetchone(conn,
        _ph("SELECT COUNT(*) as cnt FROM monthly_summary WHERE year = ? AND month = ? AND store_name = ?"),
        (year, month, store),
    )
    conn.close()
    return row["cnt"]


# ─── Revenue Data ────────────────────────────────────────────────────

def save_revenue_data(records: list[dict]):
    if not records:
        return
    conn = get_connection()
    year = records[0]["year"]
    month = records[0]["month"]
    store_name = records[0].get("store_name", "")
    _execute(conn, _ph("DELETE FROM revenue_data WHERE year = ? AND month = ? AND store_name = ?"), (year, month, store_name))

    insert_sql = """INSERT INTO revenue_data (year, month, store_name, category, amount, member_count, note)
               VALUES (:year, :month, :store_name, :category, :amount, :member_count, :note)"""

    for r in records:
        converted_sql, params = _named_to_positional(insert_sql, r)
        _execute(conn, converted_sql, params)
    conn.commit()
    conn.close()


def get_revenue_data(year: int, month: int = None, store: str = None):
    conn = get_connection()
    query = "SELECT * FROM revenue_data WHERE year = " + _param(1)
    params = [year]
    if month is not None:
        query += " AND month = " + _param(len(params) + 1)
        params.append(month)
    if store is not None and store != "全体":
        query += " AND store_name = " + _param(len(params) + 1)
        params.append(store)
    query += " ORDER BY month, store_name"
    rows = _fetchall(conn, query, tuple(params))
    conn.close()
    return [dict(r) for r in rows]


def get_revenue_months(year: int = None):
    conn = get_connection()
    if year:
        rows = _fetchall(conn,
            _ph("SELECT DISTINCT year, month FROM revenue_data WHERE year = ? ORDER BY month"),
            (year,),
        )
    else:
        rows = _fetchall(conn,
            "SELECT DISTINCT year, month FROM revenue_data ORDER BY year, month"
        )
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
    _execute(conn, _ph("DELETE FROM member_data WHERE year = ? AND month = ? AND store_name = ?"), (year, month, store_name))

    insert_sql = """INSERT INTO member_data (
                year, month, store_name, member_id, member_name,
                plan_name, join_date, tenure,
                is_active, is_new, had_trial,
                plan_end_date, trial_date, first_trial_date, initial_plan
            ) VALUES (
                :year, :month, :store_name, :member_id, :member_name,
                :plan_name, :join_date, :tenure,
                :is_active, :is_new, :had_trial,
                :plan_end_date, :trial_date, :first_trial_date, :initial_plan
            )"""

    for r in records:
        converted_sql, params = _named_to_positional(insert_sql, r)
        _execute(conn, converted_sql, params)
    conn.commit()
    conn.close()


def get_member_data(year: int, month: int = None, store: str = None):
    conn = get_connection()
    query = "SELECT * FROM member_data WHERE year = " + _param(1)
    params = [year]
    if month is not None:
        query += " AND month = " + _param(len(params) + 1)
        params.append(month)
    if store is not None and store != "全体":
        query += " AND store_name = " + _param(len(params) + 1)
        params.append(store)
    query += " ORDER BY store_name, plan_name, member_id"
    rows = _fetchall(conn, query, tuple(params))
    conn.close()
    return [dict(r) for r in rows]


def get_member_months(year: int = None):
    conn = get_connection()
    if year:
        rows = _fetchall(conn,
            _ph("SELECT DISTINCT year, month FROM member_data WHERE year = ? ORDER BY month"),
            (year,),
        )
    else:
        rows = _fetchall(conn,
            "SELECT DISTINCT year, month FROM member_data ORDER BY year, month"
        )
    conn.close()
    return [(r["year"], r["month"]) for r in rows]


def get_monthly_summary_months(year: int = None):
    conn = get_connection()
    if year:
        rows = _fetchall(conn,
            _ph("SELECT DISTINCT year, month FROM monthly_summary WHERE year = ? ORDER BY month"),
            (year,),
        )
    else:
        rows = _fetchall(conn,
            "SELECT DISTINCT year, month FROM monthly_summary ORDER BY year, month"
        )
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
    _execute(conn,
        _ph("DELETE FROM monthly_summary WHERE year = ? AND month = ? AND store_name = ?"),
        (year, month, store_name),
    )

    insert_sql = """INSERT INTO monthly_summary (
                year, month, store_name, total_members, plan_subscribers,
                plan_subscribers_1st, new_registrations, new_plan_applications,
                new_plan_signups, plan_changes, suspensions, cancellations, cancellation_rate
            ) VALUES (
                :year, :month, :store_name, :total_members, :plan_subscribers,
                :plan_subscribers_1st, :new_registrations, :new_plan_applications,
                :new_plan_signups, :plan_changes, :suspensions, :cancellations, :cancellation_rate
            )"""

    for r in records:
        converted_sql, params = _named_to_positional(insert_sql, r)
        _execute(conn, converted_sql, params)
    conn.commit()
    conn.close()


def get_monthly_summary(year: int, month: int = None, store: str = None):
    conn = get_connection()
    query = "SELECT * FROM monthly_summary WHERE year = " + _param(1)
    params = [year]
    if month is not None:
        query += " AND month = " + _param(len(params) + 1)
        params.append(month)
    if store is not None and store != "全体":
        query += " AND store_name = " + _param(len(params) + 1)
        params.append(store)
    query += " ORDER BY month, store_name"
    rows = _fetchall(conn, query, tuple(params))
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


# ─── Sales Detail Data (PL001) ──────────────────────────────────────

SALES_CATEGORIES = [
    "月会費", "入会金", "パーソナル", "オプション",
    "スポット", "体験", "ロッカー", "クーポン/割引", "その他",
]


def classify_sale_category(description: str, amount: int) -> str:
    """Auto-classify a sale description into a category."""
    if not description:
        return "その他"
    d = description
    # Check クーポン/割引 early (before 入会金 which may appear in coupon descriptions)
    if "クーポン" in d or amount < 0:
        return "クーポン/割引"
    if "月会費" in d:
        return "月会費"
    if "入会金" in d or "事務手数料" in d or "忘れ物カルテ" in d:
        return "入会金"
    if "パーソナル" in d:
        return "パーソナル"
    if "アスリート" in d or "BOOST" in d or "飲むハイアルチ" in d or "NMN" in d or "BJ" in d:
        return "オプション"
    if "スポット" in d:
        return "スポット"
    if "体験" in d:
        return "体験"
    if "ロッカー" in d:
        return "ロッカー"
    return "その他"


def save_sales_detail(records: list[dict]):
    """Save sales detail records from hacomono PL001. Replaces existing for same year/month/store."""
    if not records:
        return
    conn = get_connection()
    year = records[0]["year"]
    month = records[0]["month"]
    store_name = records[0].get("store_name", "")
    _execute(conn,
        _ph("DELETE FROM sales_detail WHERE year = ? AND month = ? AND store_name = ?"),
        (year, month, store_name),
    )

    insert_sql = """INSERT INTO sales_detail (
                year, month, store_name, sale_id, sale_date,
                payment_method, description, category,
                amount, tax, discount
            ) VALUES (
                :year, :month, :store_name, :sale_id, :sale_date,
                :payment_method, :description, :category,
                :amount, :tax, :discount
            )"""

    for r in records:
        converted_sql, params = _named_to_positional(insert_sql, r)
        _execute(conn, converted_sql, params)
    conn.commit()
    conn.close()


def get_sales_detail(year: int, month: int = None, store: str = None):
    conn = get_connection()
    query = "SELECT * FROM sales_detail WHERE year = " + _param(1)
    params = [year]
    if month is not None:
        query += " AND month = " + _param(len(params) + 1)
        params.append(month)
    if store is not None and store != "全体":
        query += " AND store_name = " + _param(len(params) + 1)
        params.append(store)
    query += " ORDER BY month, sale_date"
    rows = _fetchall(conn, query, tuple(params))
    conn.close()
    return [dict(r) for r in rows]


def get_sales_detail_months(year: int = None):
    conn = get_connection()
    if year:
        rows = _fetchall(conn,
            _ph("SELECT DISTINCT year, month FROM sales_detail WHERE year = ? ORDER BY month"),
            (year,),
        )
    else:
        rows = _fetchall(conn,
            "SELECT DISTINCT year, month FROM sales_detail ORDER BY year, month"
        )
    conn.close()
    return [(r["year"], r["month"]) for r in rows]


# ─── Data availability ───────────────────────────────────────────────

def get_available_years():
    conn = get_connection()
    rows = _fetchall(conn, """
        SELECT DISTINCT year FROM (
            SELECT year FROM payroll_data
            UNION SELECT year FROM expense_data
            UNION SELECT year FROM revenue_data
            UNION SELECT year FROM member_data
            UNION SELECT year FROM monthly_summary
            UNION SELECT year FROM sales_detail
        ) AS combined ORDER BY year
    """)
    conn.close()
    return [r["year"] for r in rows]


def get_available_months(year: int):
    conn = get_connection()
    rows = _fetchall(conn, _ph("""
        SELECT DISTINCT month FROM (
            SELECT month FROM payroll_data WHERE year = ?
            UNION SELECT month FROM expense_data WHERE year = ?
            UNION SELECT month FROM revenue_data WHERE year = ?
            UNION SELECT month FROM member_data WHERE year = ?
            UNION SELECT month FROM monthly_summary WHERE year = ?
            UNION SELECT month FROM sales_detail WHERE year = ?
        ) AS combined ORDER BY month
    """), (year, year, year, year, year, year))
    conn.close()
    return [r["month"] for r in rows]


# ─── Amazon Orders ──────────────────────────────────────────────────

AMAZON_STORE_MAP = {
    "東日本橋": "東日本橋",
    "日本橋": "東日本橋",
    "巣鴨": "巣鴨",
    "船橋": "船橋",
    "祖師ヶ谷大蔵": "祖師ヶ谷大蔵",
    "祖師谷": "祖師ヶ谷大蔵",
    "下北沢": "下北沢",
    "春日": "春日",
    "中目黒": "中目黒",
}


def detect_store_from_address(address: str) -> str:
    """Detect store name from a delivery address string."""
    if not address:
        return ""
    for keyword, store in AMAZON_STORE_MAP.items():
        if keyword in address:
            return store
    return ""


def save_amazon_orders(orders: list[dict]) -> tuple[int, int]:
    """Save Amazon orders. Returns (new_count, skip_count)."""
    if not orders:
        return 0, 0
    conn = get_connection()
    new_count = 0
    skip_count = 0

    insert_sql = """INSERT INTO amazon_orders
               (order_date, order_id, store_name, product_name, short_name,
                amount, order_total, payment_date, delivery_address)
               VALUES (:order_date, :order_id, :store_name, :product_name, :short_name,
                       :amount, :order_total, :payment_date, :delivery_address)"""

    for o in orders:
        try:
            converted_sql, params = _named_to_positional(insert_sql, o)
            _execute(conn, converted_sql, params)
            new_count += 1
        except (sqlite3.IntegrityError, Exception) as e:
            if _is_pg():
                conn.rollback()  # PG needs rollback after integrity error
            # Check if it's actually an integrity error for PG
            if _is_pg() and HAS_PSYCOPG2:
                if isinstance(e, psycopg2.IntegrityError):
                    skip_count += 1
                else:
                    raise
            elif isinstance(e, sqlite3.IntegrityError):
                skip_count += 1
            else:
                raise
    conn.commit()
    conn.close()
    return new_count, skip_count


def get_amazon_order_count() -> int:
    conn = get_connection()
    row = _fetchone(conn, "SELECT COUNT(*) as cnt FROM amazon_orders")
    conn.close()
    return row["cnt"]


def match_amazon_breakdown(description: str, amount: float, day: int, month: int, year: int, store: str) -> str:
    """Try to match an expense line to Amazon order details.

    Matching priority:
    1. payment_date + store + order_total
    2. month + store + order_total
    3. month + store + individual amount

    Returns short product names joined by ' / ' or empty string.
    """
    conn = get_connection()
    payment_date_str = f"{year}/{month:02d}/{day:02d}"
    amt = int(amount)

    # Priority 1: exact payment_date + store + order_total
    rows = _fetchall(conn,
        _ph("SELECT short_name FROM amazon_orders WHERE payment_date = ? AND store_name = ? AND order_total = ?"),
        (payment_date_str, store, amt),
    )
    if rows:
        conn.close()
        names = list(dict.fromkeys(r["short_name"] for r in rows if r["short_name"]))
        return " / ".join(names) if names else ""

    # Priority 2: month + store + order_total
    like_pattern = f"{year}/{month:02d}/%"
    rows = _fetchall(conn,
        _ph("SELECT short_name FROM amazon_orders WHERE payment_date LIKE ? AND store_name = ? AND order_total = ?"),
        (like_pattern, store, amt),
    )
    if rows:
        conn.close()
        names = list(dict.fromkeys(r["short_name"] for r in rows if r["short_name"]))
        return " / ".join(names) if names else ""

    # Priority 3: month + store + individual amount
    rows = _fetchall(conn,
        _ph("SELECT short_name FROM amazon_orders WHERE payment_date LIKE ? AND store_name = ? AND amount = ?"),
        (like_pattern, store, amt),
    )
    if rows:
        conn.close()
        names = list(dict.fromkeys(r["short_name"] for r in rows if r["short_name"]))
        return " / ".join(names) if names else ""

    conn.close()
    return ""


# ─── Auth ────────────────────────────────────────────────────────────

def _hash_password(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()


def authenticate_user(username: str, password: str) -> dict | None:
    conn = get_connection()
    row = _fetchone(conn,
        _ph("SELECT * FROM users WHERE username = ? AND password_hash = ?"),
        (username, _hash_password(password)),
    )
    conn.close()
    return dict(row) if row else None


def get_all_users() -> list[dict]:
    conn = get_connection()
    rows = _fetchall(conn,
        "SELECT id, username, role, store_name, display_name FROM users ORDER BY id"
    )
    conn.close()
    return [dict(r) for r in rows]


def create_user(username: str, password: str, role: str, store_name: str, display_name: str) -> bool:
    conn = get_connection()
    try:
        _execute(conn,
            _ph("INSERT INTO users (username, password_hash, role, store_name, display_name) VALUES (?, ?, ?, ?, ?)"),
            (username, _hash_password(password), role, store_name, display_name),
        )
        conn.commit()
        conn.close()
        return True
    except (sqlite3.IntegrityError, Exception) as e:
        if _is_pg() and HAS_PSYCOPG2:
            if isinstance(e, psycopg2.IntegrityError):
                conn.close()
                return False
            raise
        elif isinstance(e, sqlite3.IntegrityError):
            conn.close()
            return False
        raise


def delete_user(user_id: int) -> bool:
    """Delete a user. Prevents deleting the admin user."""
    conn = get_connection()
    row = _fetchone(conn, _ph("SELECT username FROM users WHERE id = ?"), (user_id,))
    if row and row["username"] == "admin":
        conn.close()
        return False
    _execute(conn, _ph("DELETE FROM users WHERE id = ?"), (user_id,))
    conn.commit()
    conn.close()
    return True


