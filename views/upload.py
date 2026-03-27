"""Data upload page — handles payroll (Excel/CSV), expense (CSV), and hacomono data."""

import streamlit as st
import pandas as pd
import openpyxl
import io
import csv
import re
from datetime import datetime, date
from database import (
    save_payroll_data, save_expense_data, save_revenue_data,
    save_member_data, save_monthly_summary,
    upsert_override, STORES, EXPENSE_CATEGORIES,
)
from store_logic import resolve_store, apply_ratio
from expense_logic import classify_expense


# hacomono store name mapping: full name → short name used in the app
HACOMONO_STORE_MAP = {
    "ハイアルチ東日本橋スタジオ": "東日本橋",
    "ハイアルチ春日スタジオ": "春日",
    "ハイアルチ船橋スタジオ": "船橋",
    "ハイアルチ巣鴨スタジオ": "巣鴨",
    "ハイアルチ祖師ヶ谷大蔵スタジオ": "祖師ヶ谷大蔵",
    "ハイアルチ下北沢スタジオ": "下北沢",
    "ハイアルチ中目黒スタジオ": "中目黒",
    "ハイアルチ東陽町スタジオ": "東陽町",
}


def _map_hacomono_store(full_name: str) -> str:
    """Map hacomono full store name to short name."""
    full_name = full_name.strip()
    if full_name in HACOMONO_STORE_MAP:
        return HACOMONO_STORE_MAP[full_name]
    # Fallback: strip prefix/suffix
    short = full_name.replace("ハイアルチ", "").replace("スタジオ", "").strip()
    return short if short else full_name


def _parse_date_loose(val: str) -> date | None:
    """Parse a date string in various formats. Returns None if unparseable or empty."""
    if not val or not val.strip():
        return None
    val = val.strip()
    for fmt in ("%Y/%m/%d %H:%M", "%Y-%m-%d %H:%M:%S", "%Y/%m/%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(val, fmt).date()
        except ValueError:
            continue
    return None


def _is_in_month(dt: date | None, year: int, month: int) -> bool:
    """Check if a date falls within the given year/month."""
    if dt is None:
        return False
    return dt.year == year and dt.month == month


def _parse_ml001_csv(
    file_bytes: bytes, year: int, month: int, target_store: str
) -> tuple[list[dict], dict]:
    """Parse hacomono ML001 member CSV.

    Returns (records, summary_info).
    CSV is UTF-8-sig with 58 columns. Uses 0-indexed column positions as fallback.
    """
    # Decode
    text = None
    for enc in ["utf-8-sig", "utf-8", "cp932"]:
        try:
            text = file_bytes.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    if text is None:
        raise ValueError("CSVのエンコーディングを判定できませんでした")

    reader = csv.reader(io.StringIO(text))
    header = next(reader)

    # Build column index map (header name → index)
    hmap = {}
    for i, h in enumerate(header):
        hmap[h.strip()] = i

    # Key column indices — try header name first, fall back to known positions
    def _col(name: str, fallback: int) -> int:
        return hmap.get(name, fallback)

    idx_member_id = _col("メンバーID", 0)
    idx_member_name = _col("氏名", 2)
    idx_trial_date = _col("無料体験会 受講日時", 37)
    idx_first_trial = _col("トライアル 初回受講日時", 38)
    idx_join_date = _col("入会日時", 39)
    idx_member_store = _col("メンバー所属店舗名", 44)
    idx_plan_name = _col("契約プラン名", 47)
    idx_current_store = _col("所属店舗名", 49)
    idx_plan_contract_date = _col("プラン契約日", 50)
    idx_plan_end_date = _col("プラン契約適用終了日", 52)
    idx_initial_plan = _col("初回契約プラン", 55)
    idx_tenure = _col("在籍期間", 56)

    today = date.today()
    records = []
    empty_store_count = 0
    total_count = 0
    active_count = 0
    suspended_count = 0
    new_count = 0
    trial_count = 0
    plan_counts = {}
    active_plan_counts = {}
    personal_ticket_count = 0

    for row in reader:
        if len(row) < 10:
            continue

        # Extract fields safely
        def _get(idx: int) -> str:
            return row[idx].strip() if idx < len(row) else ""

        member_id = _get(idx_member_id)
        member_name = _get(idx_member_name)
        plan_name = _get(idx_plan_name)

        # Skip rows with no plan name (empty rows)
        if not plan_name:
            continue

        # Determine store
        store_full = _get(idx_current_store)
        if not store_full:
            store_full = _get(idx_member_store)

        if store_full:
            store_short = _map_hacomono_store(store_full)
        else:
            # Empty store name — assign to target store (they belong to this store's export)
            store_short = target_store
            empty_store_count += 1

        total_count += 1

        # Parse dates
        trial_dt = _parse_date_loose(_get(idx_trial_date))
        first_trial_dt = _parse_date_loose(_get(idx_first_trial))
        join_dt = _parse_date_loose(_get(idx_join_date))
        plan_contract_dt = _parse_date_loose(_get(idx_plan_contract_date))
        plan_end_dt = _parse_date_loose(_get(idx_plan_end_date))

        tenure = _get(idx_tenure)
        initial_plan = _get(idx_initial_plan)
        join_date_str = _get(idx_join_date)
        trial_date_str = _get(idx_trial_date)
        first_trial_str = _get(idx_first_trial)
        plan_end_str = _get(idx_plan_end_date)

        # Determine is_active: active unless 休会 in plan name, or plan_end_date is in the past
        is_suspended = "休会" in plan_name
        has_ended = plan_end_dt is not None and plan_end_dt < today
        is_active = 0 if (is_suspended or has_ended) else 1

        # Determine is_new: tenure is "1ヶ月目" OR plan_contract_date is in the selected month
        is_new = 0
        if tenure == "1ヶ月目":
            is_new = 1
        elif _is_in_month(plan_contract_dt, year, month):
            is_new = 1

        # Determine had_trial: trial date or first trial date falls within selected month
        had_trial = 0
        if _is_in_month(trial_dt, year, month) or _is_in_month(first_trial_dt, year, month):
            had_trial = 1

        # Track パーソナルチケット
        if "パーソナル" in plan_name and "チケット" in plan_name:
            personal_ticket_count += 1

        records.append({
            "year": year,
            "month": month,
            "store_name": store_short,
            "member_id": member_id,
            "member_name": member_name,
            "plan_name": plan_name,
            "join_date": join_date_str,
            "tenure": tenure,
            "is_active": is_active,
            "is_new": is_new,
            "had_trial": had_trial,
            "plan_end_date": plan_end_str,
            "trial_date": trial_date_str,
            "first_trial_date": first_trial_str,
            "initial_plan": initial_plan,
        })

        plan_counts[plan_name] = plan_counts.get(plan_name, 0) + 1
        if is_active:
            active_count += 1
            active_plan_counts[plan_name] = active_plan_counts.get(plan_name, 0) + 1
        if is_suspended:
            suspended_count += 1
        if is_new:
            new_count += 1
        if had_trial:
            trial_count += 1

    summary_info = {
        "total": total_count,
        "active": active_count,
        "suspended": suspended_count,
        "new": new_count,
        "trial": trial_count,
        "plan_counts": plan_counts,
        "active_plan_counts": active_plan_counts,
        "empty_store_count": empty_store_count,
        "personal_ticket": personal_ticket_count,
        "store": target_store,
    }

    return records, summary_info


def _render_ml001_summary(info: dict):
    """Render summary after ML001 import."""
    st.markdown("---")
    st.markdown("### 取込結果サマリー")

    # KPI row
    k1, k2, k3, k4, k5 = st.columns(5)
    with k1:
        st.metric("全会員数", f"{info['total']}名")
    with k2:
        st.metric("有効在籍数", f"{info['active']}名")
    with k3:
        st.metric("休会", f"{info['suspended']}名")
    with k4:
        st.metric("新規入会（当月）", f"{info['new']}名")
    with k5:
        st.metric("体験（当月）", f"{info['trial']}名")

    # Plan breakdown table
    st.markdown("---")
    col_plan, col_active_plan = st.columns(2)

    with col_plan:
        st.markdown("**プラン別 全会員数**")
        if info["plan_counts"]:
            plan_data = sorted(info["plan_counts"].items(), key=lambda x: -x[1])
            plan_df = pd.DataFrame(plan_data, columns=["プラン名", "会員数"])
            total = sum(v for _, v in plan_data)
            plan_df["構成比"] = plan_df["会員数"].apply(lambda x: f"{x / total * 100:.1f}%")
            st.dataframe(plan_df, use_container_width=True, hide_index=True)

    with col_active_plan:
        st.markdown("**プラン別 有効在籍数**")
        if info["active_plan_counts"]:
            active_data = sorted(info["active_plan_counts"].items(), key=lambda x: -x[1])
            active_df = pd.DataFrame(active_data, columns=["プラン名", "会員数"])
            active_total = sum(v for _, v in active_data)
            active_df["構成比"] = active_df["会員数"].apply(lambda x: f"{x / active_total * 100:.1f}%")
            st.dataframe(active_df, use_container_width=True, hide_index=True)


def _safe_float(val) -> float:
    if val is None:
        return 0.0
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0


def _detect_year_month_from_filename(filename: str) -> tuple[int | None, int | None]:
    """Try to detect year and month from filename patterns like 2026年02月 or 202602."""
    # Pattern: 2026年02月 or 2026年2月
    m = re.search(r'(\d{4})年(\d{1,2})月', filename)
    if m:
        return int(m.group(1)), int(m.group(2))
    # Pattern: 202602
    m = re.search(r'(\d{4})(\d{2})', filename)
    if m:
        y, mo = int(m.group(1)), int(m.group(2))
        if 2020 <= y <= 2030 and 1 <= mo <= 12:
            return y, mo
    return None, None


def _parse_payroll_from_worksheet(ws, year: int, month: int) -> tuple[list[dict], list[dict]]:
    """Parse a single worksheet (payroll data) into records."""
    records = []
    unresolved = []

    for row_idx in range(2, ws.max_row + 1):
        emp_id_raw = ws.cell(row=row_idx, column=1).value
        if emp_id_raw is None:
            continue

        emp_id_str = str(emp_id_raw).strip()

        # Skip header-like rows
        if emp_id_str.startswith("【"):
            continue

        emp_name = ws.cell(row=row_idx, column=2).value or ""
        if emp_name == "-":
            continue

        contract_type = ws.cell(row=row_idx, column=6).value or ""

        # Parse all numeric fields
        work_days_weekday = _safe_float(ws.cell(row=row_idx, column=7).value)
        work_days_holiday = _safe_float(ws.cell(row=row_idx, column=8).value)
        work_days_legal = _safe_float(ws.cell(row=row_idx, column=9).value)
        scheduled_hours = _safe_float(ws.cell(row=row_idx, column=13).value)

        overtime_hours = sum(
            _safe_float(ws.cell(row=row_idx, column=c).value)
            for c in range(16, 25)
        )

        base_salary = _safe_float(ws.cell(row=row_idx, column=28).value)
        position_allowance = _safe_float(ws.cell(row=row_idx, column=29).value)
        overtime_pay = _safe_float(ws.cell(row=row_idx, column=33).value)
        commute_taxable = _safe_float(ws.cell(row=row_idx, column=45).value)
        commute_nontax = _safe_float(ws.cell(row=row_idx, column=46).value)
        taxable_total = _safe_float(ws.cell(row=row_idx, column=52).value)
        gross_total = _safe_float(ws.cell(row=row_idx, column=56).value)

        health_ins_co = _safe_float(ws.cell(row=row_idx, column=90).value)
        care_ins_co = _safe_float(ws.cell(row=row_idx, column=91).value)
        pension_co = _safe_float(ws.cell(row=row_idx, column=92).value)
        child_co = _safe_float(ws.cell(row=row_idx, column=93).value)
        pension_fund_co = _safe_float(ws.cell(row=row_idx, column=94).value)
        employment_ins_co = _safe_float(ws.cell(row=row_idx, column=95).value)
        workers_comp_co = _safe_float(ws.cell(row=row_idx, column=96).value)
        general_co = _safe_float(ws.cell(row=row_idx, column=97).value)

        store_assignments = resolve_store(emp_id_str)

        if not store_assignments:
            unresolved.append({
                "employee_id": emp_id_str,
                "employee_name": emp_name,
                "contract_type": contract_type,
                "gross_total": gross_total,
            })
            continue

        for assignment in store_assignments:
            ratio = assignment["ratio"]
            records.append({
                "year": year,
                "month": month,
                "employee_id": emp_id_str,
                "employee_name": emp_name,
                "contract_type": contract_type,
                "store_name": assignment["store_name"],
                "ratio": ratio,
                "work_days_weekday": apply_ratio(work_days_weekday, ratio),
                "work_days_holiday": apply_ratio(work_days_holiday, ratio),
                "work_days_legal_holiday": apply_ratio(work_days_legal, ratio),
                "scheduled_hours": apply_ratio(scheduled_hours, ratio),
                "overtime_hours": apply_ratio(overtime_hours, ratio),
                "base_salary": apply_ratio(base_salary, ratio),
                "position_allowance": apply_ratio(position_allowance, ratio),
                "overtime_pay": apply_ratio(overtime_pay, ratio),
                "commute_taxable": apply_ratio(commute_taxable, ratio),
                "commute_nontax": apply_ratio(commute_nontax, ratio),
                "taxable_total": apply_ratio(taxable_total, ratio),
                "gross_total": apply_ratio(gross_total, ratio),
                "health_insurance_co": apply_ratio(health_ins_co, ratio),
                "care_insurance_co": apply_ratio(care_ins_co, ratio),
                "pension_co": apply_ratio(pension_co, ratio),
                "child_contribution_co": apply_ratio(child_co, ratio),
                "pension_fund_co": apply_ratio(pension_fund_co, ratio),
                "employment_insurance_co": apply_ratio(employment_ins_co, ratio),
                "workers_comp_co": apply_ratio(workers_comp_co, ratio),
                "general_contribution_co": apply_ratio(general_co, ratio),
            })

    return records, unresolved


def parse_payroll_excel(file_bytes: bytes, year: int, month: int) -> tuple[list[dict], list[dict]]:
    """Parse payroll Excel (.xlsx) file."""
    wb = openpyxl.load_workbook(io.BytesIO(file_bytes), data_only=True)

    detail_sheet = None
    for name in wb.sheetnames:
        if "支給控除一覧表" in name:
            detail_sheet = wb[name]
            break
    if detail_sheet is None:
        detail_sheet = wb[wb.sheetnames[0]]

    return _parse_payroll_from_worksheet(detail_sheet, year, month)


def parse_payroll_csv(file_bytes: bytes, year: int, month: int) -> tuple[list[dict], list[dict]]:
    """Parse payroll CSV file. Try multiple encodings."""
    for enc in ["cp932", "utf-8", "utf-8-sig"]:
        try:
            text = file_bytes.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    else:
        raise ValueError("CSVのエンコーディングを判定できませんでした")

    reader = csv.reader(io.StringIO(text))
    header = next(reader)

    records = []
    unresolved = []

    for row in reader:
        if len(row) < 10:
            continue

        emp_id_str = str(row[0]).strip()
        if not emp_id_str or emp_id_str.startswith("【"):
            continue

        emp_name = row[1].strip() if len(row) > 1 else ""
        if emp_name == "-":
            continue

        contract_type = row[5].strip() if len(row) > 5 else ""

        # Parse numeric columns by index (same as Excel column numbers - 1)
        def col(idx):
            return _safe_float(row[idx]) if len(row) > idx else 0.0

        work_days_weekday = col(6)
        work_days_holiday = col(7)
        work_days_legal = col(8)
        scheduled_hours = col(12)
        overtime_hours = sum(col(c) for c in range(15, 24))

        base_salary = col(27)
        position_allowance = col(28)
        overtime_pay = col(32)
        commute_taxable = col(44)
        commute_nontax = col(45)
        taxable_total = col(51)
        gross_total = col(55)

        health_ins_co = col(89)
        care_ins_co = col(90)
        pension_co = col(91)
        child_co = col(92)
        pension_fund_co = col(93)
        employment_ins_co = col(94)
        workers_comp_co = col(95)
        general_co = col(96)

        store_assignments = resolve_store(emp_id_str)

        if not store_assignments:
            unresolved.append({
                "employee_id": emp_id_str,
                "employee_name": emp_name,
                "contract_type": contract_type,
                "gross_total": gross_total,
            })
            continue

        for assignment in store_assignments:
            ratio = assignment["ratio"]
            records.append({
                "year": year,
                "month": month,
                "employee_id": emp_id_str,
                "employee_name": emp_name,
                "contract_type": contract_type,
                "store_name": assignment["store_name"],
                "ratio": ratio,
                "work_days_weekday": apply_ratio(work_days_weekday, ratio),
                "work_days_holiday": apply_ratio(work_days_holiday, ratio),
                "work_days_legal_holiday": apply_ratio(work_days_legal, ratio),
                "scheduled_hours": apply_ratio(scheduled_hours, ratio),
                "overtime_hours": apply_ratio(overtime_hours, ratio),
                "base_salary": apply_ratio(base_salary, ratio),
                "position_allowance": apply_ratio(position_allowance, ratio),
                "overtime_pay": apply_ratio(overtime_pay, ratio),
                "commute_taxable": apply_ratio(commute_taxable, ratio),
                "commute_nontax": apply_ratio(commute_nontax, ratio),
                "taxable_total": apply_ratio(taxable_total, ratio),
                "gross_total": apply_ratio(gross_total, ratio),
                "health_insurance_co": apply_ratio(health_ins_co, ratio),
                "care_insurance_co": apply_ratio(care_ins_co, ratio),
                "pension_co": apply_ratio(pension_co, ratio),
                "child_contribution_co": apply_ratio(child_co, ratio),
                "pension_fund_co": apply_ratio(pension_fund_co, ratio),
                "employment_insurance_co": apply_ratio(employment_ins_co, ratio),
                "workers_comp_co": apply_ratio(workers_comp_co, ratio),
                "general_contribution_co": apply_ratio(general_co, ratio),
            })

    return records, unresolved


def parse_expense_csv(file_bytes: bytes, encoding: str = "cp932") -> list[dict]:
    """Parse PayPay銀行 CSV and return classified expense records."""
    text = file_bytes.decode(encoding)
    reader = csv.reader(io.StringIO(text))
    header = next(reader)

    records = []
    for row in reader:
        if len(row) < 12:
            continue

        year = int(row[0])
        month = int(row[1])
        day = int(row[2])
        description = row[7].strip()
        amount_str = row[8].strip()
        deposit_str = row[9].strip()

        amount = float(amount_str) if amount_str else 0.0
        deposit = float(deposit_str) if deposit_str else 0.0

        category, is_revenue = classify_expense(description)

        records.append({
            "year": year,
            "month": month,
            "day": day,
            "description": description,
            "amount": amount,
            "deposit": deposit,
            "category": category if category != "_収入" else "_収入",
            "is_revenue": 1 if is_revenue else 0,
        })

    return records


def render():
    st.header("📤 データ取込")

    tab_payroll, tab_expense, tab_revenue = st.tabs(["💰 人件費", "🧾 経費", "📈 売上"])

    # ─── 人件費 Upload ──────────────────────────────────────────
    with tab_payroll:
        st.subheader("人件費データ取込")
        st.caption("クラウド給与から出力した支給控除一覧表（Excel / CSV）をアップロード")

        uploaded_payroll = st.file_uploader(
            "ファイルをアップロード",
            type=["xlsx", "xls", "csv"],
            key="payroll_upload",
        )

        if uploaded_payroll is not None:
            filename = uploaded_payroll.name
            detected_year, detected_month = _detect_year_month_from_filename(filename)

            st.info(f"📄 **{filename}**")

            col1, col2 = st.columns(2)
            with col1:
                payroll_year = st.number_input(
                    "対象年",
                    min_value=2020, max_value=2030,
                    value=detected_year or 2026,
                    key="payroll_year",
                )
            with col2:
                payroll_month = st.number_input(
                    "対象月",
                    min_value=1, max_value=12,
                    value=detected_month or 2,
                    key="payroll_month",
                )

            if detected_year and detected_month:
                st.success(f"ファイル名から **{detected_year}年{detected_month}月** を検出しました")

            if st.button("▶ 人件費データを取り込む", type="primary", key="btn_payroll"):
                with st.spinner("解析中..."):
                    file_bytes = uploaded_payroll.read()

                    if filename.endswith(".csv"):
                        records, unresolved = parse_payroll_csv(file_bytes, payroll_year, payroll_month)
                    else:
                        records, unresolved = parse_payroll_excel(file_bytes, payroll_year, payroll_month)

                # Handle unresolved employees
                if unresolved:
                    st.warning(f"⚠️ 店舗が不明な従業員が {len(unresolved)} 名います")

                    for emp in unresolved:
                        with st.container():
                            st.markdown(f"**{emp['employee_name']}** (ID: {emp['employee_id']}, {emp['contract_type']}, ¥{emp['gross_total']:,.0f})")
                            assign_col1, assign_col2 = st.columns([3, 1])
                            with assign_col1:
                                selected_store = st.selectbox(
                                    f"店舗 — {emp['employee_name']}",
                                    STORES,
                                    key=f"assign_{emp['employee_id']}",
                                    label_visibility="collapsed",
                                )
                            with assign_col2:
                                if st.button("登録", key=f"btn_assign_{emp['employee_id']}"):
                                    emp_id = int(emp["employee_id"])
                                    upsert_override(emp_id, selected_store, 100)
                                    st.success(f"✅ {emp['employee_name']} → {selected_store}")
                                    st.rerun()

                if records:
                    save_payroll_data(records)
                    st.success(f"✅ {payroll_year}年{payroll_month}月の人件費データを取り込みました（{len(records)}件）")

                    # Summary
                    df = pd.DataFrame(records)
                    summary = df.groupby("store_name").agg(
                        人数=("employee_id", "nunique"),
                        課税支給合計=("taxable_total", "sum"),
                        総勤務時間=("scheduled_hours", "sum"),
                    ).reset_index()
                    summary.columns = ["店舗", "人数", "課税支給合計", "総勤務時間"]
                    summary["課税支給合計"] = summary["課税支給合計"].apply(lambda x: f"¥{x:,.0f}")
                    summary["総勤務時間"] = summary["総勤務時間"].apply(lambda x: f"{x:,.0f}h")
                    st.dataframe(summary, use_container_width=True, hide_index=True)
                elif not unresolved:
                    st.warning("データが見つかりませんでした。ファイルの形式を確認してください。")

    # ─── 経費 Upload ──────────────────────────────────────────
    with tab_expense:
        st.subheader("経費データ取込")
        st.caption("各店舗のPayPay銀行 入出金明細CSV（Shift-JIS）をアップロード")

        col_store, col_year, col_month = st.columns(3)
        with col_store:
            expense_store = st.selectbox("対象店舗", STORES, key="expense_store")
        with col_year:
            expense_year = st.number_input("対象年", min_value=2020, max_value=2030, value=2026, key="expense_year")
        with col_month:
            expense_month = st.number_input("対象月", min_value=1, max_value=12, value=2, key="expense_month")

        uploaded_expense = st.file_uploader(
            "CSVファイルをアップロード",
            type=["csv"],
            key="expense_upload",
        )

        if uploaded_expense is not None:
            st.info(f"📄 **{uploaded_expense.name}** → **{expense_store}** / {expense_year}年{expense_month}月")

            # Step 1: Parse CSV
            if st.button("▶ 経費データを解析する", type="primary", key="btn_expense_parse"):
                with st.spinner("解析中..."):
                    file_bytes = uploaded_expense.read()
                    try:
                        records = parse_expense_csv(file_bytes, "cp932")
                    except UnicodeDecodeError:
                        records = parse_expense_csv(file_bytes, "utf-8")

                for r in records:
                    r["year"] = expense_year
                    r["month"] = expense_month
                    r["store_name"] = expense_store

                st.session_state["expense_records"] = records
                st.session_state["expense_meta"] = {
                    "store": expense_store, "year": expense_year, "month": expense_month
                }

            # Step 2: Show parsed results and allow classification
            if "expense_records" in st.session_state:
                records = st.session_state["expense_records"]
                meta = st.session_state["expense_meta"]

                classified = [r for r in records if r["category"] is not None]
                unclassified = [r for r in records if r["category"] is None]

                st.markdown(f"**解析結果:** {len(classified)}件分類済み / {len(unclassified)}件未分類")

                # Show classified summary
                if classified:
                    with st.expander(f"✅ 分類済み（{len(classified)}件）", expanded=False):
                        df_c = pd.DataFrame(classified)
                        exp_only = df_c[df_c["is_revenue"] == 0]
                        if not exp_only.empty:
                            summary = exp_only.groupby("category")["amount"].sum().sort_values(ascending=False).reset_index()
                            summary.columns = ["勘定科目", "合計金額"]
                            summary["合計金額"] = summary["合計金額"].apply(lambda x: f"¥{x:,.0f}")
                            st.dataframe(summary, use_container_width=True, hide_index=True)

                # Show unclassified items for manual selection
                if unclassified:
                    st.warning(f"⚠️ 以下の {len(unclassified)} 件の勘定科目を選んでください")
                    cat_options = ["（未選択）"] + EXPENSE_CATEGORIES

                    for i, rec in enumerate(unclassified):
                        uc_col1, uc_col2, uc_col3 = st.columns([4, 2, 3])
                        with uc_col1:
                            st.text(f"{rec['day']}日 — {rec['description']}")
                        with uc_col2:
                            if rec["amount"] > 0:
                                st.text(f"支払: ¥{rec['amount']:,.0f}")
                            else:
                                st.text(f"入金: ¥{rec['deposit']:,.0f}")
                        with uc_col3:
                            selected_cat = st.selectbox(
                                f"科目",
                                cat_options,
                                key=f"exp_cat_{i}",
                                label_visibility="collapsed",
                            )
                            if selected_cat != "（未選択）":
                                rec["category"] = selected_cat

                # Step 3: Save button
                st.markdown("---")
                if st.button("💾 この内容で保存する", type="primary", key="btn_expense_save"):
                    all_to_save = [r for r in records]
                    saved_count = len([r for r in all_to_save if r["category"] is not None])
                    unsaved_count = len([r for r in all_to_save if r["category"] is None])

                    save_expense_data(all_to_save)
                    st.success(f"✅ **{meta['store']}** {meta['year']}年{meta['month']}月の経費データを保存しました（{saved_count}件分類済み、{unsaved_count}件未分類）")

                    # Show final summary
                    df_all = pd.DataFrame([r for r in all_to_save if r.get("is_revenue", 0) == 0 and r.get("category")])
                    if not df_all.empty:
                        summary = df_all.groupby("category")["amount"].sum().sort_values(ascending=False).reset_index()
                        summary.columns = ["勘定科目", "合計金額"]
                        summary["合計金額"] = summary["合計金額"].apply(lambda x: f"¥{x:,.0f}")
                        st.dataframe(summary, use_container_width=True, hide_index=True)

                    # Clear session state
                    del st.session_state["expense_records"]
                    del st.session_state["expense_meta"]

    # ─── 売上・会員 Upload (hacomono) ─────────────────────────
    with tab_revenue:
        st.subheader("hacomonoデータ取込")
        st.caption("hacomonoから出力したCSVをアップロード（会員リスト ML001 / 売上集計 PA002）")

        sub_member, sub_sales, sub_summary = st.tabs(["👥 会員データ (ML001)", "💰 売上データ (PA002)", "📊 月次サマリ (MA002)"])

        # ─── 会員データ (ML001) ────────────────────────────
        with sub_member:
            st.markdown("#### 会員データ取込 (ML001)")
            st.caption("hacomono「メンバー一覧」CSVをアップロード — 常に最新データに上書きされます")

            from datetime import datetime as _dt
            _now = _dt.now()
            ml_year = _now.year
            ml_month = _now.month

            ml_store = st.selectbox("対象店舗", STORES, key="ml_store")

            uploaded_ml = st.file_uploader(
                "ML001 CSVをアップロード",
                type=["csv"],
                key="ml001_upload",
            )

            if uploaded_ml is not None:
                st.info(f"📄 **{uploaded_ml.name}** → **{ml_store}**（最新データとして取込）")

            if st.button("▶ 会員データを取り込む", type="primary", key="btn_ml001"):
                if uploaded_ml is not None:
                    file_bytes = uploaded_ml.read()
                    try:
                        records, summary_info = _parse_ml001_csv(
                            file_bytes, ml_year, ml_month, ml_store
                        )

                        if records:
                            save_member_data(records)
                            st.success(
                                f"✅ **{ml_store}** の会員データを取り込みました"
                                f"（{summary_info['total']}名）"
                            )
                            if summary_info.get("empty_store_count", 0) > 0:
                                st.info(
                                    f"ℹ️ 店舗名が空の会員 {summary_info['empty_store_count']} 名も含めて取り込みました"
                                )

                            # Display summary
                            _render_ml001_summary(summary_info)
                        else:
                            st.warning("データが見つかりませんでした。ファイルの形式を確認してください。")

                    except Exception as e:
                        st.error(f"ファイルの読み込みに失敗しました: {e}")
                        import traceback
                        st.code(traceback.format_exc())
                else:
                    st.warning("CSVファイルをアップロードしてください。")

        # ─── 売上データ (PA002) ────────────────────────────
        with sub_sales:
            st.markdown("#### 売上データ取込 (PA002)")
            st.caption("hacomono「売上集計」クエリ PA002 の CSV をアップロード（店舗ごとにアップロード）")

            col_store_r, col_year_r, col_month_r = st.columns(3)
            with col_store_r:
                rev_store = st.selectbox("対象店舗", STORES, key="rev_store")
            with col_year_r:
                rev_year = st.number_input("対象年", min_value=2020, max_value=2030, value=2026, key="rev_year")
            with col_month_r:
                rev_month = st.number_input("対象月", min_value=1, max_value=12, value=2, key="rev_month")

            uploaded_pa = st.file_uploader(
                "PA002 CSVをアップロード",
                type=["csv"],
                key="pa002_upload",
            )

            if uploaded_pa is not None:
                st.info(f"📄 **{uploaded_pa.name}** → **{rev_store}** / {rev_year}年{rev_month}月")

            if st.button("▶ 売上データを取り込む", type="primary", key="btn_pa002"):
                if uploaded_pa is not None:
                    file_bytes = uploaded_pa.read()
                    try:
                        # Try encodings
                        text = None
                        for enc in ["utf-8-sig", "utf-8", "cp932"]:
                            try:
                                text = file_bytes.decode(enc)
                                break
                            except UnicodeDecodeError:
                                continue
                        if text is None:
                            raise ValueError("CSVのエンコーディングを判定できませんでした")

                        reader = csv.reader(io.StringIO(text))
                        header = next(reader)

                        # Find column indices by header name
                        col_map = {}
                        for i, h in enumerate(header):
                            col_map[h.strip()] = i

                        data_row = next(reader, None)
                        if data_row is None:
                            st.warning("データ行が見つかりませんでした。")
                        else:
                            def _get_val(col_name):
                                idx = col_map.get(col_name)
                                if idx is not None and idx < len(data_row):
                                    try:
                                        return float(data_row[idx].strip().replace(",", ""))
                                    except (ValueError, TypeError):
                                        return 0.0
                                return 0.0

                            target_ym = data_row[col_map.get("対象年月", 0)].strip() if "対象年月" in col_map else ""
                            total_sales = _get_val("[総売上] 合計")
                            sales_amount = _get_val("[売上] 合計")
                            plan_sales_count = int(_get_val("[プラン売上] 件数"))
                            plan_sales_total = _get_val("[プラン売上] 合計")
                            plan_unit_price = _get_val("[プラン売上] 会員単価")

                            # Detect year/month from 対象年月 if it looks like "202602"
                            detected_year, detected_month = None, None
                            if len(target_ym) == 6:
                                try:
                                    detected_year = int(target_ym[:4])
                                    detected_month = int(target_ym[4:])
                                except ValueError:
                                    pass

                            if detected_year and detected_month:
                                st.info(f"CSVの対象年月: **{detected_year}年{detected_month}月**")

                            st.markdown("**検出した売上データ:**")
                            info_df = pd.DataFrame([{
                                "総売上合計": f"¥{total_sales:,.0f}",
                                "売上合計": f"¥{sales_amount:,.0f}",
                                "プラン売上件数": f"{plan_sales_count}件",
                                "プラン売上合計": f"¥{plan_sales_total:,.0f}",
                                "プラン会員単価": f"¥{plan_unit_price:,.0f}",
                            }])
                            st.dataframe(info_df, use_container_width=True, hide_index=True)

                            # Use detected year/month if available, otherwise use user-selected
                            save_year = detected_year or rev_year
                            save_month = detected_month or rev_month

                            rev_records = [{
                                "year": save_year,
                                "month": save_month,
                                "store_name": rev_store,
                                "category": "売上",
                                "amount": float(total_sales),
                                "member_count": plan_sales_count,
                                "note": f"PA002 | プラン売上: ¥{plan_sales_total:,.0f} | 会員単価: ¥{plan_unit_price:,.0f}",
                            }]
                            save_revenue_data(rev_records)
                            st.success(f"✅ **{rev_store}** {save_year}年{save_month}月の売上データを保存しました")

                    except Exception as e:
                        st.error(f"ファイルの読み込みに失敗しました: {e}")
                else:
                    st.warning("CSVファイルをアップロードしてください。")

        # ─── 月次サマリ (MA002) ────────────────────────────
        with sub_summary:
            st.markdown("#### 月次サマリ取込 (MA002)")
            st.caption("hacomono「月次サマリ」クエリ MA002 の CSV をアップロード（UTF-8-sig, 1データ行）")

            col_store_ma, col_year_ma, col_month_ma = st.columns(3)
            with col_store_ma:
                ma_store = st.selectbox("対象店舗", STORES, key="ma_store")
            with col_year_ma:
                ma_year = st.number_input("対象年", min_value=2020, max_value=2030, value=2026, key="ma_year")
            with col_month_ma:
                ma_month = st.number_input("対象月", min_value=1, max_value=12, value=2, key="ma_month")

            uploaded_ma = st.file_uploader(
                "MA002 CSVをアップロード",
                type=["csv"],
                key="ma002_upload",
            )

            if uploaded_ma is not None:
                st.info(f"📄 **{uploaded_ma.name}** → **{ma_store}** / {ma_year}年{ma_month}月")

            if st.button("▶ 月次サマリを取り込む", type="primary", key="btn_ma002"):
                if uploaded_ma is not None:
                    file_bytes = uploaded_ma.read()
                    try:
                        # Decode CSV
                        text = None
                        for enc in ["utf-8-sig", "utf-8", "cp932"]:
                            try:
                                text = file_bytes.decode(enc)
                                break
                            except UnicodeDecodeError:
                                continue
                        if text is None:
                            raise ValueError("CSVのエンコーディングを判定できませんでした")

                        reader = csv.reader(io.StringIO(text))
                        header = next(reader)

                        # Build column index map
                        hmap = {}
                        for i, h in enumerate(header):
                            hmap[h.strip()] = i

                        data_row = next(reader, None)
                        if data_row is None:
                            st.warning("データ行が見つかりませんでした。")
                        else:
                            def _ma_get(col_name, default=""):
                                idx = hmap.get(col_name)
                                if idx is not None and idx < len(data_row):
                                    return data_row[idx].strip()
                                return default

                            def _ma_int(col_name):
                                val = _ma_get(col_name, "0")
                                try:
                                    return int(val.replace(",", ""))
                                except (ValueError, TypeError):
                                    return 0

                            # Auto-detect year/month from 対象年月
                            target_ym = _ma_get("対象年月")
                            detected_year_ma, detected_month_ma = None, None
                            if len(target_ym) == 6:
                                try:
                                    detected_year_ma = int(target_ym[:4])
                                    detected_month_ma = int(target_ym[4:])
                                except ValueError:
                                    pass

                            save_year_ma = detected_year_ma or ma_year
                            save_month_ma = detected_month_ma or ma_month

                            if detected_year_ma and detected_month_ma:
                                st.info(f"CSVの対象年月: **{detected_year_ma}年{detected_month_ma}月**")

                            total_members = _ma_int("店舗在籍会員数")
                            plan_subscribers = _ma_int("プラン契約者数")
                            plan_subscribers_1st = _ma_int("プラン契約者数 (1日時点)")
                            new_registrations = _ma_int("店舗在籍新規会員登録数")
                            new_plan_applications = _ma_int("プラン新規申込数")
                            new_plan_signups = _ma_int("プラン新規入会数")
                            plan_changes = _ma_int("プラン変更数")
                            suspensions = _ma_int("休会数")
                            cancellations = _ma_int("退会数")
                            cancellation_rate = _ma_get("退会率")

                            record = {
                                "year": save_year_ma,
                                "month": save_month_ma,
                                "store_name": ma_store,
                                "total_members": total_members,
                                "plan_subscribers": plan_subscribers,
                                "plan_subscribers_1st": plan_subscribers_1st,
                                "new_registrations": new_registrations,
                                "new_plan_applications": new_plan_applications,
                                "new_plan_signups": new_plan_signups,
                                "plan_changes": plan_changes,
                                "suspensions": suspensions,
                                "cancellations": cancellations,
                                "cancellation_rate": cancellation_rate,
                            }

                            save_monthly_summary([record])
                            st.success(
                                f"✅ {save_year_ma}年{save_month_ma}月 **{ma_store}** の月次サマリを取り込みました"
                            )

                            # Show summary
                            st.markdown("---")
                            st.markdown("### 取込結果サマリー")

                            k1, k2, k3 = st.columns(3)
                            with k1:
                                st.metric("在籍会員数", f"{total_members}名")
                            with k2:
                                st.metric("プラン契約者数", f"{plan_subscribers}名")
                            with k3:
                                st.metric("プラン契約者数（1日時点）", f"{plan_subscribers_1st}名")

                            k4, k5, k6 = st.columns(3)
                            with k4:
                                st.metric("新規会員登録数", f"{new_registrations}名")
                            with k5:
                                st.metric("新規申込数", f"{new_plan_applications}名")
                            with k6:
                                st.metric("新規入会数", f"{new_plan_signups}名")

                            k7, k8, k9 = st.columns(3)
                            with k7:
                                st.metric("プラン変更数", f"{plan_changes}件")
                            with k8:
                                st.metric("休会数", f"{suspensions}名")
                            with k9:
                                st.metric("退会数", f"{cancellations}名")

                            st.metric("退会率", cancellation_rate)

                    except Exception as e:
                        st.error(f"ファイルの読み込みに失敗しました: {e}")
                        import traceback
                        st.code(traceback.format_exc())
                else:
                    st.warning("CSVファイルをアップロードしてください。")
