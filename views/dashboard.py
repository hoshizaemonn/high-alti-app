"""PL Dashboard page — monthly and annual views."""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from database import (
    get_payroll_data, get_expense_data, get_revenue_data, get_member_data,
    get_available_years, get_available_months, get_member_summary_stats,
    get_monthly_summary,
    STORES, EXPENSE_CATEGORIES,
)


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
            "scheduled_hours": 0, "overtime_hours": 0,
            "employee_count": 0, "fulltime_count": 0, "parttime_count": 0,
        }

    df = pd.DataFrame(payroll_records)
    gross = df["gross_total"].sum()
    base = df["base_salary"].sum()
    position = df["position_allowance"].sum()
    overtime_pay = df["overtime_pay"].sum()
    commute = df["commute_taxable"].sum() + df["commute_nontax"].sum()
    taxable = df["taxable_total"].sum()
    sched_hours = df["scheduled_hours"].sum()
    ot_hours = df["overtime_hours"].sum()

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

    return {
        "gross_total": gross,
        "base_salary": base,
        "position_allowance": position,
        "overtime_pay": overtime_pay,
        "commute_total": commute,
        "taxable_total": taxable,
        "legal_welfare": welfare,
        "total_labor_cost": gross + welfare,
        "scheduled_hours": sched_hours,
        "overtime_hours": ot_hours,
        "employee_count": unique_emp,
        "fulltime_count": ft,
        "parttime_count": pt,
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


def _compute_revenue_summary(revenue_records: list[dict]) -> dict:
    if not revenue_records:
        return {"total": 0, "by_store": {}, "member_count": 0}

    df = pd.DataFrame(revenue_records)
    total = df["amount"].sum()
    by_store = df.groupby("store_name")["amount"].sum().to_dict()
    members = df["member_count"].sum()

    return {"total": total, "by_store": by_store, "member_count": members}


def _render_monthly(year: int, month: int, store: str):
    """Render monthly PL view."""
    payroll = get_payroll_data(year, month, store)
    expenses = get_expense_data(year, month, store)
    revenue = get_revenue_data(year, month, store)

    pay_sum = _compute_payroll_summary(payroll)
    exp_sum = _compute_expense_summary(expenses)
    rev_sum = _compute_revenue_summary(revenue)

    total_revenue = rev_sum["total"]
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

    # PL Table
    st.markdown("---")
    st.subheader("損益計算書（PL）")

    pl_rows = []

    # Revenue section
    pl_rows.append({"科目": "【売上高】", "金額": "", "_bold": True})
    if rev_sum["by_store"]:
        for s, amt in rev_sum["by_store"].items():
            pl_rows.append({"科目": f"  {s}", "金額": _fmt(amt)})
    else:
        pl_rows.append({"科目": "  売上", "金額": _fmt(total_revenue)})
    pl_rows.append({"科目": "売上合計", "金額": _fmt(total_revenue), "_bold": True})

    pl_rows.append({"科目": "", "金額": ""})

    # Labor cost section
    pl_rows.append({"科目": "【人件費】", "金額": "", "_bold": True})
    pl_rows.append({"科目": "  基本給", "金額": _fmt(pay_sum["base_salary"])})
    pl_rows.append({"科目": "  役職手当", "金額": _fmt(pay_sum["position_allowance"])})
    pl_rows.append({"科目": "  残業手当", "金額": _fmt(pay_sum["overtime_pay"])})
    pl_rows.append({"科目": "  通勤手当", "金額": _fmt(pay_sum["commute_total"])})
    other_pay = pay_sum["gross_total"] - pay_sum["base_salary"] - pay_sum["position_allowance"] - pay_sum["overtime_pay"] - pay_sum["commute_total"]
    if other_pay > 0:
        pl_rows.append({"科目": "  その他手当", "金額": _fmt(other_pay)})
    pl_rows.append({"科目": "  支給合計", "金額": _fmt(pay_sum["gross_total"]), "_bold": True})
    pl_rows.append({"科目": "  法定福利費（会社負担）", "金額": _fmt(pay_sum["legal_welfare"])})
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

    # Employee drill-down
    st.markdown("---")
    st.subheader("従業員別明細")

    if payroll:
        df_emp = pd.DataFrame(payroll)

        # Compute per-employee total cost and hourly rate
        df_emp["法定福利"] = (
            df_emp["health_insurance_co"] + df_emp["care_insurance_co"]
            + df_emp["pension_co"] + df_emp["child_contribution_co"]
            + df_emp["pension_fund_co"] + df_emp["employment_insurance_co"]
            + df_emp["workers_comp_co"] + df_emp["general_contribution_co"]
        )
        df_emp["人件費合計"] = df_emp["gross_total"] + df_emp["法定福利"]
        df_emp["総労働時間"] = df_emp["scheduled_hours"] + df_emp["overtime_hours"]
        df_emp["時給単価"] = df_emp.apply(
            lambda r: r["人件費合計"] / r["総労働時間"] if r["総労働時間"] > 0 else 0, axis=1
        )

        display_emp = df_emp[[
            "store_name", "employee_name", "contract_type",
            "base_salary", "position_allowance", "overtime_pay",
            "gross_total", "法定福利", "人件費合計",
            "scheduled_hours", "overtime_hours", "総労働時間", "時給単価",
        ]].copy()

        display_emp.columns = [
            "店舗", "氏名", "契約種別",
            "基本給", "役職手当", "残業手当",
            "支給合計", "法定福利", "人件費合計",
            "所定時間", "残業時間", "総労働時間", "時給単価",
        ]

        # Format currency columns
        for col in ["基本給", "役職手当", "残業手当", "支給合計", "法定福利", "人件費合計"]:
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

    # Expense detail
    if expenses:
        st.markdown("---")
        st.subheader("経費明細")
        df_exp = pd.DataFrame(expenses)
        df_exp_display = df_exp[df_exp["is_revenue"] == 0][["day", "description", "amount", "category"]].copy()
        df_exp_display.columns = ["日", "摘要", "金額", "勘定科目"]
        df_exp_display["金額"] = df_exp_display["金額"].apply(lambda x: f"¥{x:,.0f}")
        df_exp_display["勘定科目"] = df_exp_display["勘定科目"].fillna("未分類")
        st.dataframe(df_exp_display, use_container_width=True, hide_index=True)

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

        st.markdown("---")
        st.subheader("会員情報（MA002 月次サマリ）")

        mk1, mk2, mk3 = st.columns(3)
        with mk1:
            st.metric("在籍会員数", f"{ma_total_members}名", key=f"ma_total_{year}_{month}_{store}")
        with mk2:
            st.metric("プラン契約者数", f"{ma_plan_subscribers}名", key=f"ma_plan_{year}_{month}_{store}")
        with mk3:
            st.metric("退会率", ma_cancel_rate, key=f"ma_rate_{year}_{month}_{store}")

        mk4, mk5, mk6, mk7 = st.columns(4)
        with mk4:
            st.metric("新規入会", f"{ma_new_signups}名", key=f"ma_new_{year}_{month}_{store}")
        with mk5:
            st.metric("退会", f"{ma_cancellations}名", key=f"ma_cancel_{year}_{month}_{store}")
        with mk6:
            st.metric("休会", f"{ma_suspensions}名", key=f"ma_susp_{year}_{month}_{store}")
        with mk7:
            st.metric("新規会員登録", f"{ma_new_registrations}名", key=f"ma_reg_{year}_{month}_{store}")

        # Additional details in expander
        with st.expander("MA002 詳細", expanded=False):
            detail_data = pd.DataFrame([{
                "新規申込数": ma_new_applications,
                "プラン変更数": ma_plan_changes,
            }])
            st.dataframe(detail_data, use_container_width=True, hide_index=True, key=f"ma_detail_{year}_{month}_{store}")

            # Per-store breakdown if multiple records
            if len(ma_records) > 1:
                st.markdown("**店舗別**")
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
                st.dataframe(pd.DataFrame(store_rows), use_container_width=True, hide_index=True, key=f"ma_stores_{year}_{month}_{store}")

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


def _render_annual(year: int, store: str):
    """Render annual PL view with charts."""
    available_months = get_available_months(year)

    monthly_data = []
    for m in range(1, 13):
        payroll = get_payroll_data(year, m, store)
        expenses = get_expense_data(year, m, store)
        revenue = get_revenue_data(year, m, store)
        members = get_member_data(year, m, store)
        ma_records = get_monthly_summary(year, m, store)

        pay_sum = _compute_payroll_summary(payroll)
        exp_sum = _compute_expense_summary(expenses)
        rev_sum = _compute_revenue_summary(revenue)
        mem_sum = _compute_member_summary(members)

        total_labor = pay_sum["total_labor_cost"]
        total_expense = exp_sum["total"]
        total_rev = rev_sum["total"]

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
        # Revenue trend (stacked bar by category — for now just total since we don't have category breakdown)
        fig_rev = go.Figure()
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
        # Member count trend
        fig_mem = go.Figure()
        fig_mem.add_trace(go.Scatter(
            x=df["month_label"], y=df["member_count"],
            mode="lines+markers", name="会員数",
            line=dict(color="#4CAF50", width=3),
            marker=dict(size=8),
        ))
        fig_mem.update_layout(
            title="会員数推移", xaxis_title="月", yaxis_title="会員数",
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

    # MA002 Monthly Summary charts (preferred when available)
    has_ma_data = any(row["has_ma"] for row in monthly_data)
    if has_ma_data:
        st.markdown("---")
        st.subheader("会員データ (MA002 月次サマリ)")

        # Find latest month with MA002 data for snapshot
        latest_ma_month = None
        for row in reversed(monthly_data):
            if row["has_ma"]:
                latest_ma_month = row
                break

        if latest_ma_month:
            mak1, mak2, mak3, mak4, mak5, mak6 = st.columns(6)
            with mak1:
                st.metric("在籍会員数", f"{latest_ma_month['ma_total_members']}名",
                          help=f"{latest_ma_month['month_label']}時点",
                          key=f"ann_ma_total_{year}_{store}")
            with mak2:
                st.metric("プラン契約者数", f"{latest_ma_month['ma_plan_subscribers']}名",
                          key=f"ann_ma_plan_{year}_{store}")
            with mak3:
                st.metric("新規入会", f"{latest_ma_month['ma_new_signups']}名",
                          key=f"ann_ma_new_{year}_{store}")
            with mak4:
                st.metric("退会", f"{latest_ma_month['ma_cancellations']}名",
                          key=f"ann_ma_cancel_{year}_{store}")
            with mak5:
                st.metric("休会", f"{latest_ma_month['ma_suspensions']}名",
                          key=f"ann_ma_susp_{year}_{store}")
            with mak6:
                st.metric("退会率", latest_ma_month['ma_cancel_rate_str'],
                          key=f"ann_ma_rate_{year}_{store}")

        # MA002 trend charts
        ma_months = [row["month_label"] for row in monthly_data if row["has_ma"]]
        ma_total = [row["ma_total_members"] for row in monthly_data if row["has_ma"]]
        ma_plan_sub = [row["ma_plan_subscribers"] for row in monthly_data if row["has_ma"]]
        ma_new = [row["ma_new_signups"] for row in monthly_data if row["has_ma"]]
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
                    title="新規入会 / 退会 / 休会推移", xaxis_title="月", yaxis_title="人数",
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

    # Member data charts (from ML001)
    has_member_data = any(row["member_count_ml"] > 0 for row in monthly_data)
    if has_member_data:
        st.markdown("---")
        st.subheader("会員データ (hacomono)")

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
            "売上高", "人件費（支給合計）", "法定福利費",
            "人件費合計", "経費合計",
        ] + [cat for cat in EXPENSE_CATEGORIES if df[f"exp_{cat}"].sum() > 0] + [
            "営業利益",
        ],
    }

    for _, row in df.iterrows():
        m_label = row["month_label"]
        vals = [
            row["revenue"], row["gross_total"], row["legal_welfare"],
            row["labor_cost"], row["expense"],
        ] + [row[f"exp_{cat}"] for cat in EXPENSE_CATEGORIES if df[f"exp_{cat}"].sum() > 0] + [
            row["operating_profit"],
        ]
        table_data[m_label] = [_fmt(v) for v in vals]

    # Annual total and average
    n_data_months = len(has_data) if len(has_data) > 0 else 1
    total_vals = [
        ann_rev, df["gross_total"].sum(), df["legal_welfare"].sum(),
        ann_labor, ann_exp,
    ] + [df[f"exp_{cat}"].sum() for cat in EXPENSE_CATEGORIES if df[f"exp_{cat}"].sum() > 0] + [
        ann_profit,
    ]
    avg_vals = [v / n_data_months for v in total_vals]

    table_data["年間合計"] = [_fmt(v) for v in total_vals]
    table_data["月平均"] = [_fmt(v) for v in avg_vals]

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


def render():
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

    if selected_period == "年間":
        _render_annual(selected_year, selected_store)
    else:
        month = int(selected_period.replace("月", ""))
        _render_monthly(selected_year, month, selected_store)
