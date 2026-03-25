"""Data upload page — handles payroll (Excel/CSV) and expense (CSV)."""

import streamlit as st
import pandas as pd
import openpyxl
import io
import csv
import re
from database import (
    save_payroll_data, save_expense_data, save_revenue_data,
    upsert_override, STORES, EXPENSE_CATEGORIES,
)
from store_logic import resolve_store, apply_ratio
from expense_logic import classify_expense


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

    # ─── 売上 Upload ──────────────────────────────────────────
    with tab_revenue:
        st.subheader("売上・会員データ取込")
        st.caption("hacomonoから出力した売上・会員CSV（Shift-JIS / UTF-8）をアップロード")

        col_store_r, col_year_r, col_month_r = st.columns(3)
        with col_store_r:
            rev_store = st.selectbox("対象店舗", STORES, key="rev_store")
        with col_year_r:
            rev_year = st.number_input("対象年", min_value=2020, max_value=2030, value=2026, key="rev_year")
        with col_month_r:
            rev_month = st.number_input("対象月", min_value=1, max_value=12, value=2, key="rev_month")

        uploaded_revenue = st.file_uploader(
            "CSVファイルをアップロード",
            type=["csv", "xlsx", "xls"],
            key="revenue_upload",
        )

        if uploaded_revenue is not None:
            st.info(f"📄 **{uploaded_revenue.name}** → **{rev_store}** / {rev_year}年{rev_month}月")

        if st.button("▶ 売上データを取り込む", type="primary", key="btn_revenue"):
            if uploaded_revenue is not None:
                file_bytes = uploaded_revenue.read()
                filename = uploaded_revenue.name

                try:
                    if filename.endswith(".csv"):
                        for enc in ["cp932", "utf-8", "utf-8-sig"]:
                            try:
                                df_rev = pd.read_csv(io.BytesIO(file_bytes), encoding=enc)
                                break
                            except UnicodeDecodeError:
                                continue
                    else:
                        df_rev = pd.read_excel(io.BytesIO(file_bytes))

                    st.success(f"✅ ファイル読み込み完了（{len(df_rev)}行 × {len(df_rev.columns)}列）")
                    st.dataframe(df_rev.head(20), use_container_width=True, hide_index=True)

                    # Try to detect amount and member columns
                    total_amount = 0
                    member_count = 0

                    # Sum numeric columns that look like amounts
                    for col in df_rev.columns:
                        col_lower = str(col).lower()
                        if any(k in col_lower for k in ["売上", "金額", "amount", "収入", "合計"]):
                            total_amount += pd.to_numeric(df_rev[col], errors="coerce").fillna(0).sum()
                        if any(k in col_lower for k in ["会員", "member", "人数", "在籍"]):
                            member_count += int(pd.to_numeric(df_rev[col], errors="coerce").fillna(0).sum())

                    if total_amount > 0 or member_count > 0:
                        st.info(f"検出: 売上合計 **¥{total_amount:,.0f}** / 会員数 **{member_count}名**")

                    rev_records = [{
                        "year": rev_year,
                        "month": rev_month,
                        "store_name": rev_store,
                        "category": "売上",
                        "amount": float(total_amount),
                        "member_count": member_count,
                        "note": f"ファイル: {filename}",
                    }]
                    save_revenue_data(rev_records)
                    st.success(f"✅ **{rev_store}** {rev_year}年{rev_month}月の売上データを保存しました")

                except Exception as e:
                    st.error(f"ファイルの読み込みに失敗しました: {e}")
            else:
                st.warning("CSVファイルをアップロードしてください。")
