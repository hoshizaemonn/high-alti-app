"""PL Dashboard page — monthly and annual views."""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import io
from database import (
    get_payroll_data, get_expense_data, get_revenue_data, get_member_data,
    get_available_years, get_available_months, get_member_summary_stats,
    get_monthly_summary, get_sales_detail, SALES_CATEGORIES,
    STORES, HQ_STORE, EXPENSE_CATEGORIES,
    get_budget_data, BUDGET_ITEMS,
    get_square_sales,
)


# Mapping: 予算書の科目 → 実績データの算出方法
BUDGET_TO_ACTUAL = {
    # 売上系（PL001 sales_detail category）
    "月会費収入": {"type": "sales", "categories": ["月会費"]},
    "パーソナル・物販・その他収入": {"type": "sales", "categories": ["パーソナル", "オプション", "スポット", "入会金", "ロッカー", "その他"]},
    "サービス収入": {"type": "sales", "categories": ["体験"]},
    "自販機手数料収入": {"type": "sales", "categories": []},  # 手動入力
    # 人件費系
    "正社員・契約社員給与": {"type": "payroll", "field": "taxable_total"},
    "賞与": {"type": "payroll", "field": "bonus"},
    "通勤手当": {"type": "payroll", "field": "commute"},
    "法定福利費": {"type": "payroll", "field": "welfare"},
    # 経費系（EXPENSE_CATEGORIES と一致）
    "広告宣伝費": {"type": "expense", "category": "広告宣伝費"},
    "消耗品費": {"type": "expense", "category": "消耗品費"},
    "通信費": {"type": "expense", "category": "通信費"},
    "委託料": {"type": "expense", "category": "委託料"},
    "賃借料": {"type": "expense", "category": "賃借料"},
    "支払手数料": {"type": "expense", "category": "支払手数料"},
    "電気料": {"type": "expense", "category": "電気料"},
    "上下水道料": {"type": "expense", "category": "上下水道料"},
    "福利厚生費": {"type": "expense", "category": "福利厚生費"},
    "修繕費": {"type": "expense", "category": "修繕費"},
    "研修費": {"type": "expense", "category": "研修費"},
    "リース料": {"type": "expense", "category": "リース料"},
    "保険料": {"type": "expense", "category": "保険料"},
    "接待交際費": {"type": "expense", "category": "接待交際費"},
    "租税公課": {"type": "expense", "category": "租税公課"},
    "減価償却費": {"type": "manual"},
    "備品費": {"type": "expense", "category": "備品費"},
    "開発費償却": {"type": "manual"},
    "仕入高": {"type": "manual"},
}


def _compute_actual(item: str, year: int, month: int, store: str) -> int:
    """Compute actual value for a budget item from existing data."""
    mapping = BUDGET_TO_ACTUAL.get(item)
    if not mapping:
        return 0
    t = mapping["type"]

    if t == "sales":
        sales = get_sales_detail(year, month, store)
        cats = mapping["categories"]
        return sum(r["amount"] for r in sales if r["category"] in cats)

    elif t == "payroll":
        payroll = get_payroll_data(year, month, store)
        field = mapping["field"]
        if field == "taxable_total":
            return sum(r["taxable_total"] for r in payroll)
        elif field == "commute":
            return sum(r["commute_taxable"] + r["commute_nontax"] for r in payroll)
        elif field == "welfare":
            return sum(r["health_insurance_co"] + r["care_insurance_co"] + r["pension_co"]
                       + r["child_contribution_co"] + r["pension_fund_co"]
                       + r["employment_insurance_co"] + r["workers_comp_co"] + r["general_contribution_co"]
                       for r in payroll)
        return 0

    elif t == "expense":
        expenses = get_expense_data(year, month, store)
        cat = mapping["category"]
        return int(sum(r["amount"] for r in expenses if r.get("category") == cat and r.get("is_revenue") == 0))

    return 0


def _fmt(val: float) -> str:
    """Format yen amount."""
    if val >= 0:
        return f"¥{val:,.0f}"
    return f"-¥{abs(val):,.0f}"


def _fmt_pct(val: float) -> str:
    return f"{val:.1f}%"


def _kpi_card(label: str, value: float, delta: float = None, inverse: bool = False):
    """Render a KPI metric."""
    if delta is not None:
        st.metric(label, _fmt(value), f"{delta:+,.0f}", delta_color="inverse" if inverse else "normal")
    else:
        st.metric(label, _fmt(value))


def _compute_payroll_summary(payroll_records: list[dict]) -> dict:
    """Compute payroll aggregates from records."""
    if not payroll_records:
        return {
            "gross_total": 0, "base_salary": 0, "position_allowance": 0,
            "overtime_pay": 0, "commute_total": 0, "taxable_total": 0,
            "legal_welfare": 0, "total_labor_cost": 0,
            "scheduled_hours": 0, "overtime_hours": 0, "total_hours": 0,
            "employee_count": 0, "fulltime_count": 0, "parttime_count": 0,
            "fulltime_gross": 0, "parttime_gross": 0,
        }

    df = pd.DataFrame(payroll_records)
    taxable = df["taxable_total"].sum()
    base = df["base_salary"].sum()
    position = df["position_allowance"].sum()
    overtime_pay = df["overtime_pay"].sum()
    commute = df["commute_taxable"].sum() + df["commute_nontax"].sum()
    gross = df["gross_total"].sum()
    sched_hours = df["scheduled_hours"].sum()
    ot_hours = df["overtime_hours"].sum()
    total_hours = sched_hours + ot_hours

    welfare = (
        df["health_insurance_co"].sum()
        + df["care_insurance_co"].sum()
        + df["pension_co"].sum()
        + df["child_contribution_co"].sum()
        + df["pension_fund_co"].sum()
        + df["employment_insurance_co"].sum()
        + df["workers_comp_co"].sum()
        + df["general_contribution_co"].sum()
    )

    unique_emp = df["employee_id"].nunique()
    ft = df[df["contract_type"] == "正社員"]["employee_id"].nunique()
    pt = df[df["contract_type"] == "アルバイト"]["employee_id"].nunique()

    # 課税支給合計ベースで算出
    ft_taxable = df[df["contract_type"] == "正社員"]["taxable_total"].sum()
    pt_taxable = df[df["contract_type"] == "アルバイト"]["taxable_total"].sum()

    return {
        "gross_total": taxable,  # 課税支給合計を使用
        "base_salary": base,
        "position_allowance": position,
        "overtime_pay": overtime_pay,
        "commute_total": commute,
        "taxable_total": taxable,
        "legal_welfare": welfare,
        "total_labor_cost": taxable + welfare + commute,  # 課税支給合計 + 法定福利 + 通勤手当
        "scheduled_hours": sched_hours,
        "overtime_hours": ot_hours,
        "total_hours": total_hours,
        "employee_count": unique_emp,
        "fulltime_count": ft,
        "parttime_count": pt,
        "fulltime_gross": ft_taxable,  # 正社員の課税支給合計
        "parttime_gross": pt_taxable,  # アルバイトの課税支給合計
    }


def _compute_expense_summary(expense_records: list[dict]) -> dict:
    """Compute expense aggregates from records."""
    if not expense_records:
        return {"total": 0, "by_category": {}, "revenue_items": 0}

    df = pd.DataFrame(expense_records)
    expense_df = df[df["is_revenue"] == 0]
    revenue_df = df[df["is_revenue"] == 1]

    total = expense_df["amount"].sum()
    by_cat = {}
    if not expense_df.empty:
        grouped = expense_df.groupby("category")["amount"].sum()
        by_cat = grouped.to_dict()

    return {
        "total": total,
        "by_category": by_cat,
        "revenue_items": revenue_df["deposit"].sum(),
    }


def _compute_member_summary(member_records: list[dict]) -> dict:
    """Compute member aggregates from records (v2 — includes active/new/trial)."""
    if not member_records:
        return {
            "total": 0, "active": 0, "suspended": 0,
            "new": 0, "trial": 0,
            "by_store": {}, "by_plan": {}, "active_by_plan": {},
        }

    df = pd.DataFrame(member_records)
    total = len(df)
    by_store = df.groupby("store_name").size().to_dict()
    by_plan = df.groupby("plan_name").size().sort_values(ascending=False).to_dict()

    # v2 fields (graceful fallback if columns don't exist yet)
    active = int(df["is_active"].sum()) if "is_active" in df.columns else total
    suspended = total - active
    new_count = int(df["is_new"].sum()) if "is_new" in df.columns else 0
    trial_count = int(df["had_trial"].sum()) if "had_trial" in df.columns else 0

    active_df = df[df["is_active"] == 1] if "is_active" in df.columns else df
    active_by_plan = active_df.groupby("plan_name").size().sort_values(ascending=False).to_dict() if not active_df.empty else {}

    return {
        "total": total, "active": active, "suspended": suspended,
        "new": new_count, "trial": trial_count,
        "by_store": by_store, "by_plan": by_plan,
        "active_by_plan": active_by_plan,
    }


def _compute_sales_detail_summary(sales_records: list[dict]) -> dict:
    """Compute sales detail aggregates from PL001 records."""
    if not sales_records:
        return {"total": 0, "by_category": {}, "count": 0, "by_store": {}}

    df = pd.DataFrame(sales_records)
    total = df["amount"].sum()
    count = len(df)
    by_cat = df.groupby("category")["amount"].sum().to_dict()
    by_store = df.groupby("store_name")["amount"].sum().to_dict()

    return {"total": total, "by_category": by_cat, "count": count, "by_store": by_store}


def _compute_revenue_summary(revenue_records: list[dict]) -> dict:
    if not revenue_records:
        return {"total": 0, "by_store": {}, "member_count": 0}

    df = pd.DataFrame(revenue_records)
    total = df["amount"].sum()
    by_store = df.groupby("store_name")["amount"].sum().to_dict()
    members = df["member_count"].sum()

    return {"total": total, "by_store": by_store, "member_count": members}


def _render_monthly(year: int, month: int, store: str, show_payroll_detail: bool = True):
    """Render monthly PL view."""
    payroll = get_payroll_data(year, month, store)
    expenses = get_expense_data(year, month, store)
    revenue = get_revenue_data(year, month, store)
    sales_detail = get_sales_detail(year, month, store)

    pay_sum = _compute_payroll_summary(payroll)
    exp_sum = _compute_expense_summary(expenses)
    rev_sum = _compute_revenue_summary(revenue)
    sd_sum = _compute_sales_detail_summary(sales_detail)

    # Square sales (add on top of hacomono sales)
    square_records = get_square_sales(store=store, year=year, month=month)
    square_total = sum(r["gross_sales"] for r in square_records)

    # Use sales_detail total as revenue when available, otherwise fall back to revenue_data
    has_sales_detail = sd_sum["total"] != 0 or sd_sum["count"] > 0
    total_revenue = (sd_sum["total"] if has_sales_detail else rev_sum["total"]) + square_total
    total_labor = pay_sum["total_labor_cost"]
    total_expense = exp_sum["total"]
    operating_profit = total_revenue - total_labor - total_expense

    # KPI Cards
    st.markdown(f"### {year}年{month}月 — {store}")
    k1, k2, k3, k4 = st.columns(4)
    with k1:
        _kpi_card("売上合計", total_revenue)
    with k2:
        _kpi_card("人件費合計", total_labor, inverse=True)
    with k3:
        _kpi_card("経費合計", total_expense, inverse=True)
    with k4:
        _kpi_card("営業利益", operating_profit)

    # Sub-KPI row: sales detail key categories
    if has_sales_detail:
        sd_monthly_fee = sd_sum["by_category"].get("月会費", 0)
        sd_personal = sd_sum["by_category"].get("パーソナル", 0)
        sd_option = sd_sum["by_category"].get("オプション", 0)
        sk1, sk2, sk3 = st.columns(3)
        with sk1:
            st.metric("月会費", _fmt(sd_monthly_fee))
        with sk2:
            st.metric("パーソナル", _fmt(sd_personal))
        with sk3:
            st.metric("オプション", _fmt(sd_option))

    # Sub-KPI row: payroll breakdown and total hours (detail only)
    if show_payroll_detail and (pay_sum["total_hours"] > 0 or pay_sum["fulltime_gross"] > 0):
        sk1, sk2, sk3 = st.columns(3)
        with sk1:
            st.metric("正社員給与", _fmt(pay_sum["fulltime_gross"]))
        with sk2:
            st.metric("契約社員給与", _fmt(pay_sum["parttime_gross"]))
        with sk3:
            st.metric("総勤務時間", f"{pay_sum['total_hours']:,.1f}h")

    # PL Table
    st.markdown("---")
    st.subheader("損益計算書（PL）")

    pl_rows = []

    # Revenue section
    pl_rows.append({"科目": "【売上高】", "金額": "", "_bold": True})
    if has_sales_detail:
        # Show category breakdown from sales_detail
        for cat in SALES_CATEGORIES:
            amt = sd_sum["by_category"].get(cat, 0)
            if amt != 0:
                pl_rows.append({"科目": f"  {cat}", "金額": _fmt(amt)})
        if sd_sum["count"] > 0:
            pl_rows.append({"科目": f"  （取引件数: {sd_sum['count']}件）", "金額": ""})
    elif rev_sum["by_store"]:
        for s, amt in rev_sum["by_store"].items():
            pl_rows.append({"科目": f"  {s}", "金額": _fmt(amt)})
    else:
        pl_rows.append({"科目": "  売上", "金額": _fmt(total_revenue - square_total)})
    if square_total > 0:
        pl_rows.append({"科目": "  Square売上（物販・現地決済）", "金額": _fmt(square_total)})
    pl_rows.append({"科目": "売上合計", "金額": _fmt(total_revenue), "_bold": True})

    pl_rows.append({"科目": "", "金額": ""})

    # Labor cost section
    pl_rows.append({"科目": "【人件費】", "金額": "", "_bold": True})
    if show_payroll_detail:
        pl_rows.append({"科目": "  正社員給与", "金額": _fmt(pay_sum["fulltime_gross"])})
        pl_rows.append({"科目": "  契約社員給与", "金額": _fmt(pay_sum["parttime_gross"])})
        pl_rows.append({"科目": "  基本給", "金額": _fmt(pay_sum["base_salary"])})
        pl_rows.append({"科目": "  役職手当", "金額": _fmt(pay_sum["position_allowance"])})
        pl_rows.append({"科目": "  残業手当", "金額": _fmt(pay_sum["overtime_pay"])})
        pl_rows.append({"科目": "  通勤手当", "金額": _fmt(pay_sum["commute_total"])})
        other_pay = pay_sum["gross_total"] - pay_sum["base_salary"] - pay_sum["position_allowance"] - pay_sum["overtime_pay"] - pay_sum["commute_total"]
        if other_pay > 0:
            pl_rows.append({"科目": "  その他手当", "金額": _fmt(other_pay)})
        pl_rows.append({"科目": "  課税支給合計", "金額": _fmt(pay_sum["gross_total"]), "_bold": True})
        pl_rows.append({"科目": "  法定福利費（会社負担）", "金額": _fmt(pay_sum["legal_welfare"])})
        pl_rows.append({"科目": "  総勤務時間", "金額": f"{pay_sum['total_hours']:,.1f}h"})
    pl_rows.append({"科目": "人件費合計", "金額": _fmt(total_labor), "_bold": True})

    pl_rows.append({"科目": "", "金額": ""})

    # Expense section
    pl_rows.append({"科目": "【経費】", "金額": "", "_bold": True})
    for cat in EXPENSE_CATEGORIES:
        amt = exp_sum["by_category"].get(cat, 0)
        if amt > 0:
            pl_rows.append({"科目": f"  {cat}", "金額": _fmt(amt)})
    uncat = exp_sum["by_category"].get(None, 0)
    if uncat > 0:
        pl_rows.append({"科目": "  未分類", "金額": _fmt(uncat)})
    pl_rows.append({"科目": "経費合計", "金額": _fmt(total_expense), "_bold": True})

    pl_rows.append({"科目": "", "金額": ""})

    # Operating profit
    pl_rows.append({"科目": "【営業利益】", "金額": _fmt(operating_profit), "_bold": True})
    if total_revenue > 0:
        pl_rows.append({"科目": "  営業利益率", "金額": _fmt_pct(operating_profit / total_revenue * 100)})
        pl_rows.append({"科目": "  人件費率", "金額": _fmt_pct(total_labor / total_revenue * 100)})

    # Render as styled table
    display_rows = []
    for r in pl_rows:
        display_rows.append({"科目": r["科目"], "金額": r["金額"]})

    df_pl = pd.DataFrame(display_rows)
    st.dataframe(df_pl, use_container_width=True, hide_index=True, height=len(display_rows) * 36 + 38)

    # Payroll summary Excel download (admin only, all stores)
    if show_payroll_detail and payroll:
        all_stores_payroll = get_payroll_data(year, month, None)  # all stores
        if all_stores_payroll:
            df_all = pd.DataFrame(all_stores_payroll)
            # Exclude 本部
            df_all = df_all[df_all["store_name"] != "本部（除外）"]

            if not df_all.empty:
                stores_in_data = sorted(df_all["store_name"].unique())

                # Build summary per store
                summary_rows = []
                for s in stores_in_data:
                    ds = df_all[df_all["store_name"] == s]
                    ft = ds[ds["contract_type"] == "正社員"]
                    pt = ds[ds["contract_type"] != "正社員"]
                    welfare = (ds["health_insurance_co"].sum() + ds["care_insurance_co"].sum() +
                               ds["pension_co"].sum() + ds["child_contribution_co"].sum() +
                               ds["pension_fund_co"].sum() + ds["employment_insurance_co"].sum() +
                               ds["workers_comp_co"].sum() + ds["general_contribution_co"].sum())
                    summary_rows.append({
                        "store": s,
                        "正社員・契約社員給与": int(ds["taxable_total"].sum()),
                        "法定福利": int(welfare),
                        "通勤手当": int(ds["commute_taxable"].sum() + ds["commute_nontax"].sum()),
                        "総勤務時間": round(ds["scheduled_hours"].sum() + ds["overtime_hours"].sum(), 1),
                        "正社員給与": int(ft["taxable_total"].sum()) if not ft.empty else 0,
                        "契約社員給与": int(pt["taxable_total"].sum()) if not pt.empty else 0,
                    })

                # Build Excel-like DataFrame (stores as columns)
                items = ["正社員・契約社員給与", "法定福利", "通勤手当", "総勤務時間", "", "正社員給与", "契約社員給与"]
                excel_data = {"": items}
                for row in summary_rows:
                    s = row["store"]
                    excel_data[s] = [
                        f'{row["正社員・契約社員給与"]:,}',
                        f'{row["法定福利"]:,}',
                        f'{row["通勤手当"]:,}',
                        f'{row["総勤務時間"]:,}',
                        "",
                        f'{row["正社員給与"]:,}',
                        f'{row["契約社員給与"]:,}',
                    ]

                df_excel = pd.DataFrame(excel_data)

                # Generate Excel bytes
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine="openpyxl") as writer:
                    df_excel.to_excel(writer, index=False, sheet_name=f"{year}年{month}月_人件費サマリ")
                excel_bytes = output.getvalue()

                st.download_button(
                    "📥 人件費サマリをダウンロード（Excel）",
                    excel_bytes,
                    file_name=f"{year}{month:02d}_人件費サマリ.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="dl_payroll_excel",
                )

    # Employee drill-down (admin or own store only)
    if not show_payroll_detail:
        pass  # hide employee detail for other stores
    else:
        st.markdown("---")
        st.subheader("従業員別明細")

    if show_payroll_detail and payroll:
        df_emp = pd.DataFrame(payroll)

        # Compute per-employee total cost and hourly rate
        df_emp["法定福利"] = (
            df_emp["health_insurance_co"] + df_emp["care_insurance_co"]
            + df_emp["pension_co"] + df_emp["child_contribution_co"]
            + df_emp["pension_fund_co"] + df_emp["employment_insurance_co"]
            + df_emp["workers_comp_co"] + df_emp["general_contribution_co"]
        )
        df_emp["人件費合計"] = df_emp["taxable_total"] + df_emp["法定福利"]
        df_emp["総労働時間"] = df_emp["scheduled_hours"] + df_emp["overtime_hours"]
        df_emp["時給単価"] = df_emp.apply(
            lambda r: r["人件費合計"] / r["総労働時間"] if r["総労働時間"] > 0 else 0, axis=1
        )

        display_emp = df_emp[[
            "store_name", "employee_name", "contract_type",
            "base_salary", "position_allowance", "overtime_pay",
            "taxable_total", "法定福利", "人件費合計",
            "scheduled_hours", "overtime_hours", "総労働時間", "時給単価",
        ]].copy()

        display_emp.columns = [
            "店舗", "氏名", "契約種別",
            "基本給", "役職手当", "残業手当",
            "課税支給合計", "法定福利", "人件費合計",
            "所定時間", "残業時間", "総労働時間", "時給単価",
        ]

        # Format currency columns
        for col in ["基本給", "役職手当", "残業手当", "課税支給合計", "法定福利", "人件費合計"]:
            display_emp[col] = display_emp[col].apply(lambda x: f"¥{x:,.0f}")

        for col in ["所定時間", "残業時間", "総労働時間"]:
            display_emp[col] = display_emp[col].apply(lambda x: f"{x:.1f}h")

        display_emp["時給単価"] = display_emp["時給単価"].apply(lambda x: f"¥{x:,.0f}")

        # Group by store with expander
        for store_name in sorted(df_emp["store_name"].unique()):
            store_df = display_emp[display_emp["店舗"] == store_name].drop(columns=["店舗"])
            with st.expander(f"{store_name}（{len(store_df)}名）"):
                st.dataframe(store_df, use_container_width=True, hide_index=True)
    else:
        st.info("人件費データがありません。")

    # Budget vs Actual comparison
    if store and store != "全体":
        budget_records = get_budget_data(store=store, year=year, month=month)
        if budget_records:
            st.markdown("---")
            st.subheader("予算実績対比")

            # Build budget dict
            budget_by_item = {r["category"]: r["amount"] for r in budget_records}

            rows = []
            for item in BUDGET_ITEMS:
                budget = budget_by_item.get(item, 0)
                actual = _compute_actual(item, year, month, store)
                diff = actual - budget
                ratio = (actual / budget * 100) if budget != 0 else 0
                # Skip rows with both 0
                if budget == 0 and actual == 0:
                    continue
                rows.append({
                    "科目": item,
                    "予算": f"¥{budget:,.0f}",
                    "実績": f"¥{actual:,.0f}",
                    "予算差": f"¥{diff:,.0f}",
                    "予算比": f"{ratio:.1f}%",
                })

            if rows:
                st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
            else:
                st.info("表示できるデータがありません。")

    # Expense detail
    if expenses:
        st.markdown("---")
        # CSV download
        df_exp = pd.DataFrame(expenses)
        dl_only = df_exp[df_exp["is_revenue"] == 0].copy()
        if not dl_only.empty:
            if "breakdown" not in dl_only.columns:
                dl_only["breakdown"] = ""
            dl_only["breakdown"] = dl_only["breakdown"].fillna("")
            dl_csv = dl_only[["year", "month", "day", "description", "amount", "deposit", "category", "breakdown"]].copy()
            dl_csv.columns = ["操作日(年)", "操作日(月)", "操作日(日)", "摘要", "お支払金額", "お預り金額", "勘定科目", "内訳"]
            csv_bytes = dl_csv.to_csv(index=False, encoding="cp932", errors="replace").encode("cp932", errors="replace")
            store_label = store if store != "全体" else "全店舗"
            filename = f"{year}{month:02d}_{store_label}_経費明細.csv"
            st.subheader("経費明細")
            st.download_button("📥 経費明細をダウンロード（CSV）", csv_bytes, file_name=filename, mime="text/csv", key="dl_expense_csv")
        else:
            st.subheader("経費明細")

        # Editable expense table with breakdown
        exp_only = df_exp[df_exp["is_revenue"] == 0].copy()
        if "breakdown" not in exp_only.columns:
            exp_only["breakdown"] = ""
        exp_only["breakdown"] = exp_only["breakdown"].fillna("")

        # Fill empty breakdown markers
        def _fill_breakdown(row):
            if row["breakdown"]:
                return row["breakdown"]
            return "🔴 未入力"
        exp_only["breakdown"] = exp_only.apply(_fill_breakdown, axis=1)

        # Warn about missing Amazon breakdowns
        missing_count = len(exp_only[exp_only["breakdown"] == "🔴 未入力"])
        if missing_count > 0:
            st.warning(f"⚠ 内訳未入力: {missing_count}件")

        df_exp_edit = exp_only[["id", "day", "description", "amount", "category", "breakdown"]].copy()
        df_exp_edit.columns = ["id", "日", "摘要", "金額", "勘定科目", "内訳"]
        df_exp_edit["勘定科目"] = df_exp_edit["勘定科目"].fillna("未分類")
        df_exp_edit["金額"] = df_exp_edit["金額"].astype(float)

        category_options = list(EXPENSE_CATEGORIES) + ["未分類"]

        edited_df = st.data_editor(
            df_exp_edit,
            use_container_width=True,
            hide_index=True,
            num_rows="fixed",
            key="expense_editor",
            column_config={
                "id": None,
                "日": st.column_config.TextColumn("日", disabled=True, width="small"),
                "摘要": st.column_config.TextColumn("摘要", disabled=True),
                "金額": st.column_config.NumberColumn("金額", format="¥%,.0f", width="small"),
                "勘定科目": st.column_config.SelectboxColumn("勘定科目", options=category_options, required=True, width="small"),
                "内訳": st.column_config.TextColumn("内訳", width="large"),
            },
        )

        if st.button("💾 変更を保存", key="save_expense_edits"):
            from database import get_connection, upsert_breakdown_rule, apply_breakdown_rules_to_expense_data
            conn = get_connection()
            rules_saved = 0
            for i, row in edited_df.iterrows():
                orig = exp_only.iloc[i]
                db_id = orig["id"]
                breakdown_val = row["内訳"] if row["内訳"] not in ("—", "🔴 未入力") else ""
                orig_breakdown = str(orig.get("breakdown", "") or "")
                # Register as rule if user manually entered a breakdown
                if breakdown_val and breakdown_val != orig_breakdown:
                    upsert_breakdown_rule(orig["description"], int(orig["amount"]), breakdown_val)
                    rules_saved += 1
                conn.execute(
                    "UPDATE expense_data SET category = ?, breakdown = ?, amount = ? WHERE id = ?",
                    (row["勘定科目"], breakdown_val, row["金額"], db_id),
                )
            conn.commit()
            conn.close()
            # Apply rules to other rows (past/future data with same description+amount)
            also_applied = apply_breakdown_rules_to_expense_data()
            msg = "✅ 保存しました"
            if rules_saved > 0:
                msg += f"（{rules_saved}件の自動ルール登録）"
            if also_applied > 0:
                msg += f"（過去の類似データ{also_applied}件に自動反映）"
            st.success(msg)
            st.rerun()

    # MA002 Monthly Summary (preferred for KPIs when available)
    ma_records = get_monthly_summary(year, month, store)

    # Member data (ML001 — always used for plan breakdown details)
    members = get_member_data(year, month, store)
    mem_sum = _compute_member_summary(members)

    if ma_records:
        # Aggregate MA002 data (may have multiple store records when store == "全体")
        ma_total_members = sum(r["total_members"] for r in ma_records)
        ma_plan_subscribers = sum(r["plan_subscribers"] for r in ma_records)
        ma_new_signups = sum(r["new_plan_signups"] for r in ma_records)
        ma_cancellations = sum(r["cancellations"] for r in ma_records)
        ma_suspensions = sum(r["suspensions"] for r in ma_records)
        ma_new_registrations = sum(r["new_registrations"] for r in ma_records)
        ma_new_applications = sum(r["new_plan_applications"] for r in ma_records)
        ma_plan_changes = sum(r["plan_changes"] for r in ma_records)
        # For cancellation rate, use single record or compute weighted
        if len(ma_records) == 1:
            ma_cancel_rate = ma_records[0]["cancellation_rate"]
        else:
            ma_cancel_rate = f"{ma_cancellations / ma_plan_subscribers * 100:.1f}%" if ma_plan_subscribers > 0 else "-"

        # ML001から体験数を取得
        ma_trial_count = mem_sum.get("trial", 0)

        st.markdown("---")
        st.subheader("会員情報")

        mk1, mk2, mk3, mk4 = st.columns(4)
        with mk1:
            st.metric("在籍会員数", f"{ma_total_members}名")
        with mk2:
            st.metric("プラン契約者数", f"{ma_plan_subscribers}名")
        with mk3:
            st.metric("新規会員登録", f"{ma_new_registrations}名")
        with mk4:
            st.metric("退会率", ma_cancel_rate)

        mk5, mk6, mk7, mk8, mk9 = st.columns(5)
        with mk5:
            st.metric("新規入会", f"{ma_new_signups}名")
        with mk6:
            st.metric("新規申込", f"{ma_new_applications}名")
        with mk7:
            st.metric("退会", f"{ma_cancellations}名")
        with mk8:
            st.metric("休会", f"{ma_suspensions}名")
        with mk9:
            st.metric("プラン変更", f"{ma_plan_changes}名")

        # 体験数（ML001から）
        if ma_trial_count > 0:
            st.metric("体験", f"{ma_trial_count}名")

        # Manual edit for MA002 KPIs
        with st.expander("✏️ 会員情報を手動修正", expanded=False):
            edit_cols = st.columns(5)
            ed_total = edit_cols[0].number_input("在籍会員数", value=ma_total_members, key=f"ed_ma_total_{year}_{month}")
            ed_plan = edit_cols[1].number_input("プラン契約者数", value=ma_plan_subscribers, key=f"ed_ma_plan_{year}_{month}")
            ed_signups = edit_cols[2].number_input("新規入会", value=ma_new_signups, key=f"ed_ma_signups_{year}_{month}")
            ed_apps = edit_cols[3].number_input("新規申込", value=ma_new_applications, key=f"ed_ma_apps_{year}_{month}")
            ed_cancel = edit_cols[4].number_input("退会", value=ma_cancellations, key=f"ed_ma_cancel_{year}_{month}")

            edit_cols2 = st.columns(4)
            ed_suspend = edit_cols2[0].number_input("休会", value=ma_suspensions, key=f"ed_ma_suspend_{year}_{month}")
            ed_plan_chg = edit_cols2[1].number_input("プラン変更", value=ma_plan_changes, key=f"ed_ma_planchg_{year}_{month}")
            ed_new_reg = edit_cols2[2].number_input("新規登録", value=ma_new_registrations, key=f"ed_ma_newreg_{year}_{month}")
            ed_cancel_rate = edit_cols2[3].text_input("退会率", value=ma_cancel_rate, key=f"ed_ma_crate_{year}_{month}")

            if st.button("💾 会員情報を保存", key=f"btn_save_ma_{year}_{month}"):
                from database import save_monthly_summary
                record = {
                    "year": year,
                    "month": month,
                    "store_name": store if store != "全体" else ma_records[0]["store_name"],
                    "total_members": ed_total,
                    "plan_subscribers": ed_plan,
                    "plan_subscribers_1st": 0,
                    "new_registrations": ed_new_reg,
                    "new_plan_applications": ed_apps,
                    "new_plan_signups": ed_signups,
                    "plan_changes": ed_plan_chg,
                    "suspensions": ed_suspend,
                    "cancellations": ed_cancel,
                    "cancellation_rate": ed_cancel_rate,
                }
                save_monthly_summary([record])
                st.success("✅ 会員情報を保存しました")
                st.rerun()

        # Per-store breakdown if multiple records
        if len(ma_records) > 1:
            with st.expander("店舗別内訳", expanded=False):
                store_rows = []
                for r in ma_records:
                    store_rows.append({
                        "店舗": r["store_name"],
                        "在籍会員数": r["total_members"],
                        "プラン契約者数": r["plan_subscribers"],
                        "新規入会": r["new_plan_signups"],
                        "退会": r["cancellations"],
                        "休会": r["suspensions"],
                        "退会率": r["cancellation_rate"],
                    })
                st.dataframe(pd.DataFrame(store_rows), use_container_width=True, hide_index=True)

        # Still show ML001 plan breakdown if available
        if mem_sum["total"] > 0:
            mc1, mc2 = st.columns(2)
            with mc1:
                st.markdown("**プラン別会員数（有効在籍 — ML001）**")
                if mem_sum["active_by_plan"]:
                    plan_data = sorted(mem_sum["active_by_plan"].items(), key=lambda x: -x[1])
                    plan_df = pd.DataFrame(plan_data, columns=["プラン名", "会員数"])
                    total_active = sum(v for _, v in plan_data)
                    plan_df["構成比"] = plan_df["会員数"].apply(
                        lambda x: f"{x / total_active * 100:.1f}%" if total_active > 0 else "0%"
                    )
                    st.dataframe(plan_df, use_container_width=True, hide_index=True, key=f"plan_tbl_{year}_{month}_{store}")

            with mc2:
                if mem_sum["active_by_plan"]:
                    fig_plan = go.Figure(data=[go.Pie(
                        labels=list(mem_sum["active_by_plan"].keys()),
                        values=list(mem_sum["active_by_plan"].values()),
                        hole=0.4,
                    )])
                    fig_plan.update_layout(
                        title="プラン構成比（有効在籍）", height=350,
                        margin=dict(l=20, r=20, t=40, b=20),
                    )
                    st.plotly_chart(fig_plan, use_container_width=True, key=f"chart_plan_pie_{year}_{month}_{store}")

    elif mem_sum["total"] > 0:
        # Fallback: no MA002 data, use ML001 computed stats (original behavior)
        st.markdown("---")
        st.subheader("会員情報")

        # KPI cards for member data
        mk1, mk2, mk3, mk4 = st.columns(4)
        with mk1:
            st.metric("全会員数", f"{mem_sum['total']}名")
        with mk2:
            st.metric("有効在籍数", f"{mem_sum['active']}名")
        with mk3:
            st.metric("新規入会（当月）", f"{mem_sum['new']}名")
        with mk4:
            st.metric("体験（当月）", f"{mem_sum['trial']}名")

        if mem_sum["suspended"] > 0:
            st.caption(f"休会: {mem_sum['suspended']}名")

        # Estimate churn: check previous month data
        prev_month = month - 1
        prev_year = year
        if prev_month < 1:
            prev_month = 12
            prev_year = year - 1
        prev_members = get_member_data(prev_year, prev_month, store)
        if prev_members:
            prev_sum = _compute_member_summary(prev_members)
            if prev_sum["total"] > 0:
                estimated_churn = prev_sum["total"] - mem_sum["total"] + mem_sum["new"]
                if estimated_churn > 0:
                    st.caption(f"推定退会数（前月比）: {estimated_churn}名")

        mc1, mc2 = st.columns(2)
        with mc1:
            st.markdown("**プラン別会員数（有効在籍）**")
            if mem_sum["active_by_plan"]:
                plan_data = sorted(mem_sum["active_by_plan"].items(), key=lambda x: -x[1])
                plan_df = pd.DataFrame(plan_data, columns=["プラン名", "会員数"])
                total_active = sum(v for _, v in plan_data)
                plan_df["構成比"] = plan_df["会員数"].apply(
                    lambda x: f"{x / total_active * 100:.1f}%" if total_active > 0 else "0%"
                )
                st.dataframe(plan_df, use_container_width=True, hide_index=True, key=f"plan_tbl_{year}_{month}_{store}")

        with mc2:
            if len(mem_sum["by_store"]) > 1:
                st.markdown("**店舗別会員数**")
                store_df = pd.DataFrame(
                    [{"店舗": k, "会員数": v} for k, v in sorted(mem_sum["by_store"].items(), key=lambda x: -x[1])]
                )
                st.dataframe(store_df, use_container_width=True, hide_index=True, key=f"store_tbl_{year}_{month}_{store}")
            elif mem_sum["active_by_plan"]:
                # Single store — show plan pie chart
                fig_plan = go.Figure(data=[go.Pie(
                    labels=list(mem_sum["active_by_plan"].keys()),
                    values=list(mem_sum["active_by_plan"].values()),
                    hole=0.4,
                )])
                fig_plan.update_layout(
                    title="プラン構成比（有効在籍）", height=350,
                    margin=dict(l=20, r=20, t=40, b=20),
                )
                st.plotly_chart(fig_plan, use_container_width=True, key=f"chart_plan_pie_{year}_{month}_{store}")


def _render_annual(year: int, store: str, show_payroll_detail: bool = True):
    """Render annual PL view with charts."""
    available_months = get_available_months(year)

    monthly_data = []
    for m in range(1, 13):
        payroll = get_payroll_data(year, m, store)
        expenses = get_expense_data(year, m, store)
        revenue = get_revenue_data(year, m, store)
        sales_detail = get_sales_detail(year, m, store)
        members = get_member_data(year, m, store)
        ma_records = get_monthly_summary(year, m, store)

        pay_sum = _compute_payroll_summary(payroll)
        exp_sum = _compute_expense_summary(expenses)
        rev_sum = _compute_revenue_summary(revenue)
        sd_sum = _compute_sales_detail_summary(sales_detail)
        mem_sum = _compute_member_summary(members)

        total_labor = pay_sum["total_labor_cost"]
        total_expense = exp_sum["total"]
        has_sd = sd_sum["total"] != 0 or sd_sum["count"] > 0
        total_rev = sd_sum["total"] if has_sd else rev_sum["total"]

        # MA002 aggregated values
        ma_total_members = sum(r["total_members"] for r in ma_records) if ma_records else 0
        ma_plan_subscribers = sum(r["plan_subscribers"] for r in ma_records) if ma_records else 0
        ma_new_signups = sum(r["new_plan_signups"] for r in ma_records) if ma_records else 0
        ma_cancellations = sum(r["cancellations"] for r in ma_records) if ma_records else 0
        ma_suspensions = sum(r["suspensions"] for r in ma_records) if ma_records else 0
        ma_new_registrations = sum(r["new_registrations"] for r in ma_records) if ma_records else 0
        if ma_records:
            if len(ma_records) == 1:
                ma_cancel_rate_str = ma_records[0]["cancellation_rate"]
            else:
                ma_cancel_rate_str = f"{ma_cancellations / ma_plan_subscribers * 100:.1f}%" if ma_plan_subscribers > 0 else "-"
            try:
                ma_cancel_rate_num = float(ma_cancel_rate_str.replace("%", ""))
            except (ValueError, TypeError):
                ma_cancel_rate_num = 0.0
        else:
            ma_cancel_rate_str = ""
            ma_cancel_rate_num = 0.0

        monthly_data.append({
            "month": m,
            "month_label": f"{m}月",
            "revenue": total_rev,
            "labor_cost": total_labor,
            "expense": total_expense,
            "operating_profit": total_rev - total_labor - total_expense,
            "gross_total": pay_sum["gross_total"],
            "fulltime_gross": pay_sum["fulltime_gross"],
            "parttime_gross": pay_sum["parttime_gross"],
            "total_hours": pay_sum["total_hours"],
            "legal_welfare": pay_sum["legal_welfare"],
            "member_count": rev_sum["member_count"],
            "member_count_ml": mem_sum["total"],
            "member_active_ml": mem_sum.get("active", 0),
            "member_new_ml": mem_sum.get("new", 0),
            "member_trial_ml": mem_sum.get("trial", 0),
            "member_suspended_ml": mem_sum.get("suspended", 0),
            "member_by_store": mem_sum["by_store"],
            "member_by_plan": mem_sum["by_plan"],
            "member_active_by_plan": mem_sum.get("active_by_plan", {}),
            "employee_count": pay_sum["employee_count"],
            "fulltime_count": pay_sum["fulltime_count"],
            "parttime_count": pay_sum["parttime_count"],
            # MA002 fields
            "ma_total_members": ma_total_members,
            "ma_plan_subscribers": ma_plan_subscribers,
            "ma_new_signups": ma_new_signups,
            "ma_cancellations": ma_cancellations,
            "ma_suspensions": ma_suspensions,
            "ma_new_registrations": ma_new_registrations,
            "ma_cancel_rate_str": ma_cancel_rate_str,
            "ma_cancel_rate_num": ma_cancel_rate_num,
            "has_ma": bool(ma_records),
            "has_sd": has_sd,
            **{f"sd_{cat}": sd_sum["by_category"].get(cat, 0) for cat in SALES_CATEGORIES},
            **{f"exp_{cat}": exp_sum["by_category"].get(cat, 0) for cat in EXPENSE_CATEGORIES},
        })

    df = pd.DataFrame(monthly_data)
    has_data = df[df[["revenue", "labor_cost", "expense"]].sum(axis=1) > 0]

    # Annual KPIs
    ann_rev = df["revenue"].sum()
    ann_labor = df["labor_cost"].sum()
    ann_exp = df["expense"].sum()
    ann_profit = ann_rev - ann_labor - ann_exp

    st.markdown(f"### {year}年 年間 — {store}")
    k1, k2, k3, k4 = st.columns(4)
    with k1:
        _kpi_card("売上合計（年間）", ann_rev)
    with k2:
        _kpi_card("人件費合計（年間）", ann_labor, inverse=True)
    with k3:
        _kpi_card("経費合計（年間）", ann_exp, inverse=True)
    with k4:
        _kpi_card("営業利益（年間）", ann_profit)

    if has_data.empty:
        st.info("表示するデータがありません。データをアップロードしてください。")
        return

    # Charts
    st.markdown("---")
    c1, c2 = st.columns(2)

    with c1:
        has_any_sd = any(row["has_sd"] for row in monthly_data)
        fig_rev = go.Figure()
        if has_any_sd:
            # Stacked bar by sales category
            sd_colors = {
                "月会費": "#2196F3", "パーソナル": "#4CAF50", "オプション": "#FF9800",
                "入会金": "#9C27B0", "スポット": "#00BCD4", "体験": "#E91E63",
                "ロッカー": "#795548", "クーポン/割引": "#F44336", "その他": "#607D8B",
            }
            for cat in SALES_CATEGORIES:
                col_name = f"sd_{cat}"
                if col_name in df.columns and df[col_name].sum() != 0:
                    fig_rev.add_trace(go.Bar(
                        x=df["month_label"], y=df[col_name],
                        name=cat, marker_color=sd_colors.get(cat, "#607D8B"),
                    ))
            fig_rev.update_layout(barmode="stack")
        else:
            fig_rev.add_trace(go.Bar(
                x=df["month_label"], y=df["revenue"],
                name="売上", marker_color="#2196F3",
            ))
        fig_rev.update_layout(
            title="売上推移", xaxis_title="月", yaxis_title="金額（円）",
            height=350, margin=dict(l=20, r=20, t=40, b=20),
        )
        st.plotly_chart(fig_rev, use_container_width=True, key=f"chart_rev_{year}_{store}")

    with c2:
        # Member count trend (MA002 plan subscribers)
        fig_mem = go.Figure()
        fig_mem.add_trace(go.Scatter(
            x=df["month_label"], y=df["ma_plan_subscribers"],
            mode="lines+markers", name="プラン契約者数",
            line=dict(color="#4CAF50", width=3),
            marker=dict(size=8),
        ))
        fig_mem.update_layout(
            title="プラン契約者数推移", xaxis_title="月", yaxis_title="人数",
            height=350, margin=dict(l=20, r=20, t=40, b=20),
        )
        st.plotly_chart(fig_mem, use_container_width=True, key=f"chart_mem_{year}_{store}")

    c3, c4 = st.columns(2)

    with c3:
        # Revenue vs Labor cost
        fig_rl = go.Figure()
        fig_rl.add_trace(go.Scatter(
            x=df["month_label"], y=df["revenue"],
            mode="lines+markers", name="売上",
            line=dict(color="#2196F3", width=3),
        ))
        fig_rl.add_trace(go.Scatter(
            x=df["month_label"], y=df["labor_cost"],
            mode="lines+markers", name="人件費",
            line=dict(color="#F44336", width=3),
        ))
        fig_rl.update_layout(
            title="売上 vs 人件費", xaxis_title="月", yaxis_title="金額（円）",
            height=350, margin=dict(l=20, r=20, t=40, b=20),
        )
        st.plotly_chart(fig_rl, use_container_width=True, key=f"chart_rl_{year}_{store}")

    with c4:
        # Store comparison (operating profit) — only meaningful for 全体
        if store == "全体":
            store_profits = {}
            for s in STORES:
                s_payroll = get_payroll_data(year, store=s)
                s_expenses = get_expense_data(year, store=s)
                s_rev = get_revenue_data(year, store=s)
                s_pay_sum = _compute_payroll_summary(s_payroll)
                s_exp_sum = _compute_expense_summary(s_expenses)
                s_rev_sum = _compute_revenue_summary(s_rev)
                profit = s_rev_sum["total"] - s_pay_sum["total_labor_cost"] - s_exp_sum["total"]
                store_profits[s] = profit

            fig_store = go.Figure()
            colors = ["#F44336" if v < 0 else "#4CAF50" for v in store_profits.values()]
            fig_store.add_trace(go.Bar(
                x=list(store_profits.keys()),
                y=list(store_profits.values()),
                marker_color=colors,
            ))
            fig_store.update_layout(
                title="店舗別 営業利益",
                xaxis_title="店舗", yaxis_title="営業利益（円）",
                height=350, margin=dict(l=20, r=20, t=40, b=20),
            )
            st.plotly_chart(fig_store, use_container_width=True, key=f"chart_store_{year}_{store}")
        else:
            # Single store — show expense breakdown pie
            exp_cats = {cat: df[f"exp_{cat}"].sum() for cat in EXPENSE_CATEGORIES if df[f"exp_{cat}"].sum() > 0}
            if exp_cats:
                fig_pie = go.Figure(data=[go.Pie(
                    labels=list(exp_cats.keys()),
                    values=list(exp_cats.values()),
                    hole=0.4,
                )])
                fig_pie.update_layout(
                    title="経費内訳", height=350,
                    margin=dict(l=20, r=20, t=40, b=20),
                )
                st.plotly_chart(fig_pie, use_container_width=True, key=f"chart_pie_{year}_{store}")

    # ===== 会員データ（MA002 + ML001 統合セクション）=====
    has_ma_data = any(row["has_ma"] for row in monthly_data)
    has_member_data_ml = any(row["member_count_ml"] > 0 for row in monthly_data)

    if has_ma_data or has_member_data_ml:
        st.markdown("---")
        from datetime import datetime as _dt
        st.subheader(f"会員データ（{_dt.now().strftime('%Y年%-m月%-d日')}現在）")

    if has_ma_data:
        # MA002 trend charts
        ma_months = [row["month_label"] for row in monthly_data if row["has_ma"]]
        ma_total = [row["ma_total_members"] for row in monthly_data if row["has_ma"]]
        ma_plan_sub = [row["ma_plan_subscribers"] for row in monthly_data if row["has_ma"]]
        ma_new = [row["ma_new_signups"] for row in monthly_data if row["has_ma"]]
        ma_new_reg = [row.get("ma_new_registrations", 0) for row in monthly_data if row["has_ma"]]
        ma_cancel = [row["ma_cancellations"] for row in monthly_data if row["has_ma"]]
        ma_susp = [row["ma_suspensions"] for row in monthly_data if row["has_ma"]]
        ma_rate = [row["ma_cancel_rate_num"] for row in monthly_data if row["has_ma"]]

        if len(ma_months) > 0:
            mac1, mac2 = st.columns(2)

            with mac1:
                fig_ma_member = go.Figure()
                fig_ma_member.add_trace(go.Scatter(
                    x=ma_months, y=ma_total,
                    mode="lines+markers", name="在籍会員数",
                    line=dict(color="#2196F3", width=3),
                    marker=dict(size=8),
                ))
                fig_ma_member.add_trace(go.Scatter(
                    x=ma_months, y=ma_plan_sub,
                    mode="lines+markers", name="プラン契約者数",
                    line=dict(color="#4CAF50", width=3),
                    marker=dict(size=8),
                ))
                fig_ma_member.update_layout(
                    title="在籍会員数推移（MA002）", xaxis_title="月", yaxis_title="人数",
                    height=380, margin=dict(l=20, r=20, t=40, b=20),
                )
                st.plotly_chart(fig_ma_member, use_container_width=True, key=f"chart_ma_member_{year}_{store}")

            with mac2:
                fig_ma_churn = go.Figure()
                fig_ma_churn.add_trace(go.Bar(
                    x=ma_months, y=ma_new_reg,
                    name="新規会員登録", marker_color="#2196F3",
                    text=ma_new_reg, textposition="auto",
                ))
                fig_ma_churn.add_trace(go.Bar(
                    x=ma_months, y=ma_new,
                    name="新規入会", marker_color="#4CAF50",
                    text=ma_new, textposition="auto",
                ))
                fig_ma_churn.add_trace(go.Bar(
                    x=ma_months, y=ma_cancel,
                    name="退会", marker_color="#F44336",
                    text=ma_cancel, textposition="auto",
                ))
                fig_ma_churn.add_trace(go.Bar(
                    x=ma_months, y=ma_susp,
                    name="休会", marker_color="#FF9800",
                    text=ma_susp, textposition="auto",
                ))
                fig_ma_churn.update_layout(
                    title="新規登録 / 入会 / 退会 / 休会推移", xaxis_title="月", yaxis_title="人数",
                    barmode="group",
                    height=380, margin=dict(l=20, r=20, t=40, b=20),
                )
                st.plotly_chart(fig_ma_churn, use_container_width=True, key=f"chart_ma_churn_{year}_{store}")

            # Cancellation rate trend
            if any(r > 0 for r in ma_rate):
                mac3, mac4 = st.columns(2)
                with mac3:
                    fig_ma_rate = go.Figure()
                    fig_ma_rate.add_trace(go.Scatter(
                        x=ma_months, y=ma_rate,
                        mode="lines+markers", name="退会率",
                        line=dict(color="#F44336", width=3),
                        marker=dict(size=8),
                        text=[f"{r:.1f}%" for r in ma_rate],
                        textposition="top center",
                    ))
                    fig_ma_rate.update_layout(
                        title="退会率推移（MA002）", xaxis_title="月", yaxis_title="退会率（%）",
                        height=350, margin=dict(l=20, r=20, t=40, b=20),
                    )
                    st.plotly_chart(fig_ma_rate, use_container_width=True, key=f"chart_ma_rate_{year}_{store}")

    # ML001 member detail (continues within the same section)
    if has_member_data_ml:

        # Find latest month with data for snapshot
        latest_member_month = None
        for row in reversed(monthly_data):
            if row["member_count_ml"] > 0:
                latest_member_month = row
                break

        # KPI row for latest month
        if latest_member_month:
            mk1, mk2, mk3, mk4 = st.columns(4)
            with mk1:
                st.metric("全会員数", f"{latest_member_month['member_count_ml']}名",
                          help=f"{latest_member_month['month_label']}時点")
            with mk2:
                st.metric("有効在籍数", f"{latest_member_month['member_active_ml']}名")
            with mk3:
                st.metric("新規入会", f"{latest_member_month['member_new_ml']}名")
            with mk4:
                st.metric("体験", f"{latest_member_month['member_trial_ml']}名")

        mc1, mc2 = st.columns(2)

        with mc1:
            # Member count trend (line chart)
            ml_months = [row["month_label"] for row in monthly_data if row["member_count_ml"] > 0]
            ml_total = [row["member_count_ml"] for row in monthly_data if row["member_count_ml"] > 0]
            ml_active = [row["member_active_ml"] for row in monthly_data if row["member_count_ml"] > 0]

            if ml_months:
                fig_ml_trend = go.Figure()
                fig_ml_trend.add_trace(go.Scatter(
                    x=ml_months, y=ml_total,
                    mode="lines+markers", name="全会員数",
                    line=dict(color="#2196F3", width=3),
                    marker=dict(size=8),
                ))
                fig_ml_trend.add_trace(go.Scatter(
                    x=ml_months, y=ml_active,
                    mode="lines+markers", name="有効在籍数",
                    line=dict(color="#4CAF50", width=3),
                    marker=dict(size=8),
                ))
                fig_ml_trend.update_layout(
                    title="会員数推移", xaxis_title="月", yaxis_title="会員数",
                    height=380, margin=dict(l=20, r=20, t=40, b=20),
                )
                st.plotly_chart(fig_ml_trend, use_container_width=True, key=f"chart_ml_trend_{year}_{store}")

        with mc2:
            # Plan breakdown (horizontal bar) — use latest month with data
            plan_data = latest_member_month.get("member_active_by_plan", {}) if latest_member_month else {}
            if not plan_data:
                plan_data = latest_member_month.get("member_by_plan", {}) if latest_member_month else {}

            if plan_data:
                plans_sorted = sorted(plan_data.items(), key=lambda x: x[1])
                fig_plan = go.Figure()
                fig_plan.add_trace(go.Bar(
                    y=[p[0] for p in plans_sorted],
                    x=[p[1] for p in plans_sorted],
                    orientation="h",
                    marker_color="#4CAF50",
                    text=[p[1] for p in plans_sorted],
                    textposition="auto",
                ))
                fig_plan.update_layout(
                    title=f"プラン別会員数（{latest_member_month['month_label']}）",
                    xaxis_title="会員数", yaxis_title="",
                    height=380, margin=dict(l=20, r=20, t=40, b=20),
                )
                st.plotly_chart(fig_plan, use_container_width=True, key=f"chart_plan_bar_{year}_{store}")

        # New member trend by month (bar chart)
        ml_new_months = [row["month_label"] for row in monthly_data if row["member_count_ml"] > 0]
        ml_new_counts = [row["member_new_ml"] for row in monthly_data if row["member_count_ml"] > 0]
        ml_trial_counts = [row["member_trial_ml"] for row in monthly_data if row["member_count_ml"] > 0]

        if ml_new_months and any(c > 0 for c in ml_new_counts + ml_trial_counts):
            mc3, mc4 = st.columns(2)

            with mc3:
                fig_new = go.Figure()
                fig_new.add_trace(go.Bar(
                    x=ml_new_months, y=ml_new_counts,
                    name="新規入会", marker_color="#FF9800",
                    text=ml_new_counts, textposition="auto",
                ))
                fig_new.update_layout(
                    title="新規入会数推移", xaxis_title="月", yaxis_title="人数",
                    height=350, margin=dict(l=20, r=20, t=40, b=20),
                )
                st.plotly_chart(fig_new, use_container_width=True, key=f"chart_new_trend_{year}_{store}")

            with mc4:
                # Store breakdown (bar) — only for 全体 view
                if store == "全体" and latest_member_month and latest_member_month["member_by_store"]:
                    by_store = latest_member_month["member_by_store"]
                    stores_sorted = sorted(by_store.items(), key=lambda x: -x[1])
                    fig_mem_store = go.Figure()
                    fig_mem_store.add_trace(go.Bar(
                        x=[s[0] for s in stores_sorted],
                        y=[s[1] for s in stores_sorted],
                        marker_color="#2196F3",
                        text=[s[1] for s in stores_sorted],
                        textposition="auto",
                    ))
                    fig_mem_store.update_layout(
                        title=f"店舗別会員数（{latest_member_month['month_label']}）",
                        xaxis_title="店舗", yaxis_title="会員数",
                        height=350, margin=dict(l=20, r=20, t=40, b=20),
                    )
                    st.plotly_chart(fig_mem_store, use_container_width=True, key=f"chart_mem_store_{year}_{store}")
                else:
                    fig_trial = go.Figure()
                    fig_trial.add_trace(go.Bar(
                        x=ml_new_months, y=ml_trial_counts,
                        name="体験", marker_color="#9C27B0",
                        text=ml_trial_counts, textposition="auto",
                    ))
                    fig_trial.update_layout(
                        title="体験数推移", xaxis_title="月", yaxis_title="人数",
                        height=350, margin=dict(l=20, r=20, t=40, b=20),
                    )
                    st.plotly_chart(fig_trial, use_container_width=True, key=f"chart_trial_trend_{year}_{store}")

    # Annual cumulative PL table
    st.markdown("---")
    st.subheader("年間PL一覧")

    table_data = {
        "科目": [
            "売上高", "正社員給与", "契約社員給与",
            "人件費（課税支給合計）", "法定福利費",
            "人件費合計", "総勤務時間", "経費合計",
        ] + [cat for cat in EXPENSE_CATEGORIES if df[f"exp_{cat}"].sum() > 0] + [
            "営業利益",
        ],
    }

    for _, row in df.iterrows():
        m_label = row["month_label"]
        vals = [
            row["revenue"], row["fulltime_gross"], row["parttime_gross"],
            row["gross_total"], row["legal_welfare"],
            row["labor_cost"],
        ]
        # total_hours formatted differently (not yen)
        hours_str = f"{row['total_hours']:,.1f}h" if row["total_hours"] > 0 else "-"
        remaining_vals = [row["expense"]] + [row[f"exp_{cat}"] for cat in EXPENSE_CATEGORIES if df[f"exp_{cat}"].sum() > 0] + [
            row["operating_profit"],
        ]
        table_data[m_label] = [_fmt(v) for v in vals] + [hours_str] + [_fmt(v) for v in remaining_vals]

    # Annual total and average
    n_data_months = len(has_data) if len(has_data) > 0 else 1
    total_vals_pre = [
        ann_rev, df["fulltime_gross"].sum(), df["parttime_gross"].sum(),
        df["gross_total"].sum(), df["legal_welfare"].sum(),
        ann_labor,
    ]
    total_hours = df["total_hours"].sum()
    total_vals_post = [ann_exp] + [df[f"exp_{cat}"].sum() for cat in EXPENSE_CATEGORIES if df[f"exp_{cat}"].sum() > 0] + [
        ann_profit,
    ]
    avg_vals_pre = [v / n_data_months for v in total_vals_pre]
    avg_hours = total_hours / n_data_months
    avg_vals_post = [v / n_data_months for v in total_vals_post]

    table_data["年間合計"] = [_fmt(v) for v in total_vals_pre] + [f"{total_hours:,.1f}h"] + [_fmt(v) for v in total_vals_post]
    table_data["月平均"] = [_fmt(v) for v in avg_vals_pre] + [f"{avg_hours:,.1f}h"] + [_fmt(v) for v in avg_vals_post]

    df_table = pd.DataFrame(table_data)
    st.dataframe(df_table, use_container_width=True, hide_index=True)

    # Headcount summary
    st.markdown("---")
    st.subheader("人員推移")
    hc_data = {
        "項目": ["正社員", "アルバイト", "合計"],
    }
    for _, row in df.iterrows():
        hc_data[row["month_label"]] = [
            row["fulltime_count"], row["parttime_count"], row["employee_count"]
        ]
    df_hc = pd.DataFrame(hc_data)
    st.dataframe(df_hc, use_container_width=True, hide_index=True)


def _can_view_payroll_detail(user: dict, viewing_store: str) -> bool:
    """Check if user can see individual employee breakdown and payroll line items."""
    if not user or user.get("role") == "admin":
        return True
    # Store managers can see detail only for their own store
    return viewing_store == user.get("store_name")


def render(user=None):
    if user is None:
        user = {"role": "admin", "store_name": None}

    years = get_available_years()
    if not years:
        st.info("データがまだアップロードされていません。「アップロード」ページからデータを取り込んでください。")
        return

    # Selectors in columns
    col_year, col_period, col_store = st.columns(3)

    with col_year:
        selected_year = st.selectbox("年度", years, index=len(years) - 1, key="dash_year")

    period_options = ["年間"] + [f"{m}月" for m in range(1, 13)]

    with col_period:
        selected_period = st.selectbox("期間", period_options, key="dash_period")

    store_options = STORES + ["全体"]
    with col_store:
        selected_store = st.selectbox("店舗", store_options, key="dash_store")

    st.markdown("---")

    show_payroll_detail = _can_view_payroll_detail(user, selected_store)

    if selected_period == "年間":
        _render_annual(selected_year, selected_store, show_payroll_detail=show_payroll_detail)
    else:
        month = int(selected_period.replace("月", ""))
        _render_monthly(selected_year, month, selected_store, show_payroll_detail=show_payroll_detail)
