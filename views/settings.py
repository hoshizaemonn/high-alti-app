"""Settings page — manage store overrides and expense classification rules."""

import streamlit as st
import pandas as pd
from database import (
    get_all_overrides, upsert_override, delete_override,
    get_all_expense_rules, upsert_expense_rule, delete_expense_rule,
    get_connection,
    STORES, EXPENSE_CATEGORIES,
)


def _get_employee_names() -> dict:
    """Get employee_id -> name mapping from payroll data."""
    conn = get_connection()
    rows = conn.execute(
        "SELECT DISTINCT employee_id, employee_name FROM payroll_data WHERE employee_name != ''"
    ).fetchall()
    conn.close()
    return {str(r["employee_id"]): r["employee_name"] for r in rows}


def render():
    st.header("設定")

    tab_store, tab_expense = st.tabs(["従業員→店舗マッピング", "経費分類ルール"])

    # ─── Store Override Settings ─────────────────────────────────
    with tab_store:
        st.subheader("従業員→店舗 割り当てテーブル")
        st.caption(
            "ここに登録がない場合、従業員番号の千の位で店舗を自動判定します。"
            "（1xxx→東日本橋, 2xxx→春日, 3xxx→船橋, 4xxx→巣鴨, 6xxx→祖師ヶ谷大蔵, 7xxx→下北沢, 8xxx→中目黒）"
        )

        overrides = get_all_overrides()
        emp_names = _get_employee_names()

        if overrides:
            # Build editable dataframe
            edit_data = []
            for r in overrides:
                emp_id_str = str(r["employee_id"])
                edit_data.append({
                    "id": r["id"],
                    "従業員番号": r["employee_id"],
                    "氏名": emp_names.get(emp_id_str, "—"),
                    "店舗": r["store_name"],
                    "比率(%)": r["ratio"],
                    "削除": False,
                })

            df = pd.DataFrame(edit_data)

            edited_df = st.data_editor(
                df[["従業員番号", "氏名", "店舗", "比率(%)", "削除"]],
                column_config={
                    "従業員番号": st.column_config.NumberColumn("従業員番号", disabled=True),
                    "氏名": st.column_config.TextColumn("氏名", disabled=True),
                    "店舗": st.column_config.SelectboxColumn("店舗", options=STORES, required=True),
                    "比率(%)": st.column_config.NumberColumn("比率(%)", min_value=1, max_value=100),
                    "削除": st.column_config.CheckboxColumn("削除", default=False),
                },
                use_container_width=True,
                hide_index=True,
                num_rows="fixed",
                key="override_editor",
            )

            if st.button("💾 変更を保存", type="primary", key="btn_save_overrides"):
                deleted = 0
                updated = 0

                for i, row in edited_df.iterrows():
                    orig = edit_data[i]
                    db_id = orig["id"]

                    # Delete checked rows
                    if row["削除"]:
                        delete_override(db_id)
                        deleted += 1
                        continue

                    # Check if store or ratio changed
                    if row["店舗"] != orig["店舗"] or row["比率(%)"] != orig["比率(%)"]:
                        # Delete old and insert new
                        delete_override(db_id)
                        upsert_override(orig["従業員番号"], row["店舗"], int(row["比率(%)"]))
                        updated += 1

                msgs = []
                if updated > 0:
                    msgs.append(f"{updated}件更新")
                if deleted > 0:
                    msgs.append(f"{deleted}件削除")
                if msgs:
                    st.success(f"✅ {', '.join(msgs)}しました")
                    st.rerun()
                else:
                    st.info("変更はありません")

        else:
            st.info("登録がありません。")

        # Add new
        st.markdown("---")
        st.markdown("#### 新規追加")
        add_col1, add_col2, add_col3, add_col4, add_col5 = st.columns([2, 2, 2, 2, 1])
        with add_col1:
            new_emp_id = st.number_input("従業員番号", min_value=1, step=1, key="new_override_emp")
        with add_col2:
            new_emp_name = st.text_input("氏名", key="new_override_name")
        with add_col3:
            new_store = st.selectbox("店舗", STORES, key="new_override_store")
        with add_col4:
            new_ratio = st.number_input("比率(%)", min_value=1, max_value=100, value=100, key="new_override_ratio")
        with add_col5:
            st.markdown("<br>", unsafe_allow_html=True)
            if st.button("追加", type="primary", key="btn_add_override"):
                upsert_override(int(new_emp_id), new_store, int(new_ratio))
                st.success(f"✅ {new_emp_name}({int(new_emp_id)}) → {new_store} ({int(new_ratio)}%)")
                st.rerun()

        # Dual assignment helper
        st.markdown("---")
        st.markdown("#### 兼務登録（既存の従業員に店舗を追加）")
        st.caption("上のテーブルに既にいる従業員に、2つ目の店舗を追加します。比率の合計が100%になるよう設定してください。")

        # Show existing overrides for selection
        if overrides:
            existing_emps = {}
            for r in overrides:
                eid = r["employee_id"]
                name = emp_names.get(str(eid), "")
                if eid not in existing_emps:
                    existing_emps[eid] = {"name": name, "stores": [], "records": []}
                existing_emps[eid]["stores"].append(f"{r['store_name']}({r['ratio']}%)")
                existing_emps[eid]["records"].append(r)

            emp_options = [f"{eid} — {info['name']} [{', '.join(info['stores'])}]" for eid, info in existing_emps.items()]

            dual_col1, dual_col2, dual_col3 = st.columns([3, 2, 2])
            with dual_col1:
                selected_emp = st.selectbox("対象従業員", emp_options, key="dual_emp_select")
                dual_emp_id = int(selected_emp.split(" — ")[0])
            with dual_col2:
                # Exclude stores already assigned
                current_stores = [r["store_name"] for r in existing_emps[dual_emp_id]["records"]]
                available_stores = [s for s in STORES if s not in current_stores]
                if available_stores:
                    dual_store2 = st.selectbox("追加する店舗", available_stores, key="dual_store2")
                else:
                    dual_store2 = st.selectbox("追加する店舗", STORES, key="dual_store2")
                    st.caption("※ 全店舗に既に登録済みです")
            with dual_col3:
                dual_ratio2 = st.number_input("新しい店舗の比率(%)", min_value=1, max_value=99, value=40, key="dual_ratio2")

            # Show what will happen
            current_records = existing_emps[dual_emp_id]["records"]
            remaining_ratio = 100 - dual_ratio2

            if len(current_records) == 1:
                orig = current_records[0]
                st.info(
                    f"**変更内容:** {orig['store_name']} {orig['ratio']}% → **{remaining_ratio}%** / "
                    f"{dual_store2} → **{dual_ratio2}%**（合計100%）"
                )

            if st.button("兼務登録", type="primary", key="btn_dual"):
                # Auto-adjust existing store ratio
                if len(current_records) == 1:
                    orig = current_records[0]
                    # Delete old 100% record and re-insert with adjusted ratio
                    delete_override(orig["id"])
                    upsert_override(dual_emp_id, orig["store_name"], remaining_ratio)

                # Add new store
                upsert_override(dual_emp_id, dual_store2, int(dual_ratio2))

                if len(current_records) == 1:
                    orig = current_records[0]
                    st.success(
                        f"✅ 従業員{dual_emp_id}: "
                        f"{orig['store_name']}({remaining_ratio}%) + {dual_store2}({dual_ratio2}%)"
                    )
                else:
                    st.success(f"✅ 従業員{dual_emp_id} → {dual_store2}({dual_ratio2}%) を追加しました")
                st.rerun()

    # ─── Expense Classification Rules ────────────────────────────
    with tab_expense:
        st.subheader("経費自動分類ルール")
        st.caption("PayPay銀行CSVの「摘要」に含まれるキーワードで勘定科目を自動判定します。")

        rules = get_all_expense_rules()

        if rules:
            rule_data = []
            for r in rules:
                rule_data.append({
                    "id": r["id"],
                    "キーワード": r["keyword"],
                    "勘定科目": r["category"],
                    "削除": False,
                })

            df_rules = pd.DataFrame(rule_data)

            edited_rules = st.data_editor(
                df_rules[["キーワード", "勘定科目", "削除"]],
                column_config={
                    "キーワード": st.column_config.TextColumn("キーワード"),
                    "勘定科目": st.column_config.SelectboxColumn(
                        "勘定科目", options=EXPENSE_CATEGORIES + ["_収入"], required=True
                    ),
                    "削除": st.column_config.CheckboxColumn("削除", default=False),
                },
                use_container_width=True,
                hide_index=True,
                num_rows="fixed",
                key="rules_editor",
            )

            if st.button("💾 変更を保存", type="primary", key="btn_save_rules"):
                deleted = 0
                updated = 0

                for i, row in edited_rules.iterrows():
                    orig = rule_data[i]
                    db_id = orig["id"]

                    if row["削除"]:
                        delete_expense_rule(db_id)
                        deleted += 1
                        continue

                    if row["キーワード"] != orig["キーワード"] or row["勘定科目"] != orig["勘定科目"]:
                        delete_expense_rule(db_id)
                        upsert_expense_rule(row["キーワード"], row["勘定科目"])
                        updated += 1

                msgs = []
                if updated > 0:
                    msgs.append(f"{updated}件更新")
                if deleted > 0:
                    msgs.append(f"{deleted}件削除")
                if msgs:
                    st.success(f"✅ {', '.join(msgs)}しました")
                    st.rerun()
                else:
                    st.info("変更はありません")

        else:
            st.info("ルールがありません。")

        # Add new rule
        st.markdown("---")
        st.markdown("#### 新規ルール追加")
        rule_col1, rule_col2, rule_col3 = st.columns([3, 3, 1])
        with rule_col1:
            new_keyword = st.text_input("キーワード", key="new_rule_keyword")
        with rule_col2:
            cat_options = EXPENSE_CATEGORIES + ["_収入"]
            new_category = st.selectbox("勘定科目", cat_options, key="new_rule_category")
        with rule_col3:
            st.markdown("<br>", unsafe_allow_html=True)
            if st.button("追加", type="primary", key="btn_add_rule"):
                if new_keyword.strip():
                    upsert_expense_rule(new_keyword.strip(), new_category)
                    st.success(f"✅ 「{new_keyword.strip()}」→ {new_category}")
                    st.rerun()
                else:
                    st.error("キーワードを入力してください。")
