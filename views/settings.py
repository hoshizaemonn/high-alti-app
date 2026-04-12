"""Settings page — manage store overrides and expense classification rules."""

import streamlit as st
import pandas as pd
import io
import csv

from database import (
    get_all_overrides, upsert_override, delete_override,
    get_all_expense_rules, upsert_expense_rule, delete_expense_rule,
    get_all_users, create_user, delete_user,
    get_all_product_master, upsert_product_master, delete_product_master,
    get_product_master_category,
    AMAZON_CATEGORY_DEFAULT_MAP,
    get_connection,
    STORES, HQ_STORE, EXPENSE_CATEGORIES,
)

# Store options including HQ for employee mapping
STORE_OPTIONS_WITH_HQ = STORES + [HQ_STORE]


def _get_employee_names() -> dict:
    """Get employee_id -> name mapping from payroll data."""
    conn = get_connection()
    rows = conn.execute(
        "SELECT DISTINCT employee_id, employee_name FROM payroll_data WHERE employee_name != ''"
    ).fetchall()
    conn.close()
    return {str(r["employee_id"]): r["employee_name"] for r in rows}


def render(user=None):
    st.header("設定")

    is_admin = user and user.get("role") == "admin"

    if is_admin:
        tab_store, tab_expense, tab_amazon, tab_users = st.tabs(["従業員→店舗マッピング", "経費分類ルール", "Amazon商品マスタ", "ユーザー管理"])
    else:
        tab_store, tab_expense, tab_amazon = st.tabs(["従業員→店舗マッピング", "経費分類ルール", "Amazon商品マスタ"])

    # ─── Store Override Settings ─────────────────────────────────
    with tab_store:
        st.subheader("従業員→店舗 割り当てテーブル")
        st.caption(
            "ここに登録がない場合、従業員番号の千の位で店舗を自動判定します。"
            "（1xxx→東日本橋, 2xxx→春日, 3xxx→船橋, 4xxx→巣鴨, 6xxx→祖師ヶ谷大蔵, 7xxx→下北沢, 8xxx→中目黒）"
        )

        overrides = get_all_overrides()
        emp_names = _get_employee_names()

        # Auto-register all employees button
        override_ids = set(int(r["employee_id"]) for r in overrides)
        all_emp_ids = set(int(eid) for eid in emp_names.keys())
        unregistered = all_emp_ids - override_ids

        if unregistered:
            from database import THOUSAND_DIGIT_MAP
            st.warning(f"⚠️ 給与データに {len(unregistered)}名の未登録従業員がいます（千の位ルールで自動判定中）")
            if st.button(f"▶ 未登録 {len(unregistered)}名を一括登録", type="primary", key="btn_auto_register_all"):
                registered = 0
                skipped = 0
                for eid in sorted(unregistered):
                    td = eid // 1000
                    store = THOUSAND_DIGIT_MAP.get(td, "")
                    if store:
                        upsert_override(eid, store, 100)
                        registered += 1
                    else:
                        skipped += 1
                msg = f"✅ {registered}名を登録しました"
                if skipped:
                    msg += f"（{skipped}名は店舗判定できずスキップ）"
                st.success(msg)
                st.rerun()

        search_query = st.text_input("🔍 従業員検索（番号 or 氏名）", key="emp_search", placeholder="例: 4005 or 田中")

        if overrides:
            # Filter by search query
            filtered_overrides = overrides
            if search_query and search_query.strip():
                q = search_query.strip()
                filtered_overrides = [
                    r for r in overrides
                    if q in str(r["employee_id"]) or q in emp_names.get(str(r["employee_id"]), "")
                ]

            # Build editable dataframe
            edit_data = []
            for r in filtered_overrides:
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
                    "店舗": st.column_config.SelectboxColumn("店舗", options=STORE_OPTIONS_WITH_HQ, required=True),
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

        # Build employee lookup for add/dual
        existing_emps = {}
        for r in overrides:
            eid = r["employee_id"]
            name = emp_names.get(str(eid), "")
            if eid not in existing_emps:
                existing_emps[eid] = {"name": name, "stores": [], "records": []}
            existing_emps[eid]["stores"].append({"store": r["store_name"], "ratio": r["ratio"], "id": r["id"]})
            existing_emps[eid]["records"].append(r)

        # Add new / dual assignment — unified
        st.markdown("---")
        with st.expander("➕ 従業員の追加・兼務登録", expanded=False):
            add_mode = st.radio("登録タイプ", ["既存の従業員（兼務・変更）", "新規従業員"], horizontal=True, key="add_mode")

            if add_mode == "既存の従業員（兼務・変更）":
                emp_search_add = st.text_input("従業員を検索（名前 or 番号）", key="emp_search_add")

                if emp_search_add:
                    matches = []
                    for eid, info in existing_emps.items():
                        if emp_search_add in str(eid) or emp_search_add in info["name"]:
                            stores_str = ", ".join(f"{s['store']}({s['ratio']}%)" for s in info["stores"])
                            matches.append({"id": eid, "name": info["name"], "stores_str": stores_str, "info": info})

                    if matches:
                        options = [f"{m['id']} — {m['name']} [{m['stores_str']}]" for m in matches]
                        selected = st.selectbox("該当する従業員", options, key="existing_emp_select")
                        selected_eid = int(selected.split(" — ")[0])
                        selected_info = existing_emps[selected_eid]

                        current_label = ", ".join(f"{s['store']}({s['ratio']}%)" for s in selected_info["stores"])
                        st.markdown(f"**現在の登録:** {current_label}")

                        col_store, col_ratio = st.columns([3, 2])
                        with col_store:
                            current_stores = [s["store"] for s in selected_info["stores"]]
                            available = [s for s in STORE_OPTIONS_WITH_HQ if s not in current_stores]
                            if available:
                                add_store = st.selectbox("追加する店舗", available, key="existing_add_store")
                            else:
                                add_store = st.selectbox("追加する店舗", STORE_OPTIONS_WITH_HQ, key="existing_add_store")
                                st.caption("※ 全店舗に登録済み")
                        with col_ratio:
                            add_ratio = st.number_input("比率(%)", min_value=1, max_value=99, value=40, key="existing_add_ratio")

                        if len(selected_info["stores"]) == 1:
                            old = selected_info["stores"][0]
                            remaining = 100 - add_ratio
                            st.info(f"**変更内容:** {old['store']} {old['ratio']}% → **{remaining}%** / {add_store} → **{add_ratio}%**（合計100%）")

                        if st.button("登録", type="primary", key="btn_existing_add"):
                            if len(selected_info["stores"]) == 1:
                                old = selected_info["stores"][0]
                                remaining = 100 - int(add_ratio)
                                delete_override(old["id"])
                                upsert_override(selected_eid, old["store"], remaining)
                            upsert_override(selected_eid, add_store, int(add_ratio))
                            st.success("✅ 登録しました")
                            st.rerun()
                    else:
                        st.warning(f"「{emp_search_add}」に一致する従業員がいません")
                else:
                    st.caption("名前や番号を入力してください")

            else:
                # New employee
                col_id, col_name = st.columns(2)
                with col_id:
                    new_emp_id = st.number_input("従業員番号", min_value=1, step=1, key="new_override_emp")
                with col_name:
                    new_emp_name = st.text_input("氏名", key="new_override_name")

                has_dual = st.checkbox("兼務あり（2店舗に所属）", key="new_emp_dual")

                if not has_dual:
                    col_store, col_ratio = st.columns([3, 2])
                    with col_store:
                        new_store = st.selectbox("店舗", STORE_OPTIONS_WITH_HQ, key="new_override_store")
                    with col_ratio:
                        new_ratio = st.number_input("比率(%)", min_value=1, max_value=100, value=100, key="new_override_ratio")

                    if st.button("追加", type="primary", key="btn_add_override"):
                        upsert_override(int(new_emp_id), new_store, int(new_ratio))
                        st.success(f"✅ {new_emp_name or new_emp_id} → {new_store} ({int(new_ratio)}%)")
                        st.rerun()
                else:
                    st.caption("2店舗の比率合計が100%になるよう設定してください")
                    col_s1, col_r1, col_s2, col_r2 = st.columns([3, 1, 3, 1])
                    with col_s1:
                        store_a = st.selectbox("店舗A", STORE_OPTIONS_WITH_HQ, key="new_store_a")
                    with col_r1:
                        ratio_a = st.number_input("比率A(%)", min_value=1, max_value=99, value=60, key="new_ratio_a")
                    with col_s2:
                        store_b_options = [s for s in STORE_OPTIONS_WITH_HQ if s != store_a]
                        store_b = st.selectbox("店舗B", store_b_options, key="new_store_b")
                    with col_r2:
                        ratio_b = 100 - ratio_a
                        st.metric("比率B(%)", f"{ratio_b}%")

                    st.info(f"**{store_a}({ratio_a}%) + {store_b}({ratio_b}%)** = 合計100%")

                    if st.button("追加", type="primary", key="btn_add_dual_new"):
                        upsert_override(int(new_emp_id), store_a, int(ratio_a))
                        upsert_override(int(new_emp_id), store_b, int(ratio_b))
                        st.success(f"✅ {new_emp_name or new_emp_id} → {store_a}({ratio_a}%) + {store_b}({ratio_b}%)")
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

    # ─── Amazon Product Master ───────────────────────────────────
    with tab_amazon:
        st.subheader("Amazon商品マスタ")
        st.caption(
            "ASINごとに勘定科目を記録。Amazon注文CSV取込時に自動学習されます。"
            "ここで手動編集・追加・削除も可能です。"
        )

        masters = get_all_product_master()

        if masters:
            search_asin = st.text_input("🔍 検索（ASIN or 商品名）", key="master_search", placeholder="例: B08XXX or マスク")

            filtered = masters
            if search_asin and search_asin.strip():
                q = search_asin.strip().lower()
                filtered = [
                    m for m in masters
                    if q in m.get("asin", "").lower() or q in m.get("product_name", "").lower()
                ]

            master_data = []
            for m in filtered:
                master_data.append({
                    "id": m["id"],
                    "ASIN": m["asin"],
                    "商品名": m.get("product_name", "")[:40],
                    "Amazonカテゴリ": m.get("amazon_category", ""),
                    "勘定科目": m.get("expense_category", ""),
                    "最終取込日": m.get("last_seen_date", ""),
                    "削除": False,
                })

            df_master = pd.DataFrame(master_data)

            edited_master = st.data_editor(
                df_master[["ASIN", "商品名", "Amazonカテゴリ", "勘定科目", "最終取込日", "削除"]],
                column_config={
                    "ASIN": st.column_config.TextColumn("ASIN", disabled=True),
                    "商品名": st.column_config.TextColumn("商品名", disabled=True, width="large"),
                    "Amazonカテゴリ": st.column_config.TextColumn("Amazonカテゴリ", disabled=True),
                    "勘定科目": st.column_config.SelectboxColumn("勘定科目", options=EXPENSE_CATEGORIES, required=True),
                    "最終取込日": st.column_config.TextColumn("最終取込日", disabled=True),
                    "削除": st.column_config.CheckboxColumn("削除", default=False),
                },
                use_container_width=True,
                hide_index=True,
                num_rows="fixed",
                key="master_editor",
            )

            if st.button("💾 変更を保存", type="primary", key="btn_save_master"):
                deleted = 0
                updated = 0
                for i, row in edited_master.iterrows():
                    orig = master_data[i]
                    db_id = orig["id"]

                    if row["削除"]:
                        delete_product_master(db_id)
                        deleted += 1
                        continue

                    if row["勘定科目"] != orig["勘定科目"]:
                        m = filtered[i]
                        upsert_product_master(
                            m["asin"], m.get("product_name", ""),
                            m.get("amazon_category", ""), row["勘定科目"],
                        )
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

            st.metric("登録済み商品数", f"{len(masters)}件")
        else:
            st.info("商品マスタは空です。Amazon注文CSVを取り込むと自動で登録されます。")

        # CSV bulk import to master
        st.markdown("---")
        with st.expander("📥 注文履歴CSVから一括登録", expanded=False):
            st.caption(
                "過去のAmazonビジネス注文履歴CSVをアップロードすると、"
                "ASIN・商品名・カテゴリを商品マスタに一括登録します（注文データは保存しません）。"
                "既にマスタに登録済みのASINは勘定科目を上書きしません。"
            )
            master_csv = st.file_uploader("Amazon注文履歴CSV", type=["csv"], key="master_csv_upload")
            apply_default_cat = st.checkbox(
                "Amazonカテゴリから勘定科目の初期値を自動セット",
                value=True,
                key="master_csv_apply_default",
            )
            if master_csv is not None:
                if st.button("▶ 商品マスタに一括登録", type="primary", key="btn_master_csv_import"):
                    raw = master_csv.read()
                    text = None
                    for enc in ["utf-8-sig", "utf-8", "cp932"]:
                        try:
                            text = raw.decode(enc)
                            break
                        except UnicodeDecodeError:
                            continue
                    if text is None:
                        st.error("CSVのエンコーディングを判定できませんでした")
                    else:
                        reader = csv.DictReader(io.StringIO(text))
                        fieldnames = [f.strip().strip('\ufeff') for f in (reader.fieldnames or [])]
                        reader.fieldnames = fieldnames

                        if 'ASIN' not in fieldnames:
                            st.error("ASIN列が見つかりません。Amazonビジネスの注文履歴CSVを使用してください。")
                        else:
                            new_count = 0
                            skip_count = 0
                            for row in reader:
                                asin = (row.get('ASIN') or '').strip()
                                if not asin:
                                    continue
                                existing = get_product_master_category(asin)
                                if existing:
                                    skip_count += 1
                                    continue
                                product_name = (row.get('商品名') or '').strip()
                                amazon_cat = (row.get('商品カテゴリー') or '').strip()
                                expense_cat = ""
                                if apply_default_cat:
                                    expense_cat = AMAZON_CATEGORY_DEFAULT_MAP.get(amazon_cat, "")
                                if not expense_cat:
                                    expense_cat = "消耗品費"
                                upsert_product_master(asin, product_name, amazon_cat, expense_cat)
                                new_count += 1
                            st.success(f"✅ 新規 {new_count}件を商品マスタに登録（{skip_count}件は登録済みのためスキップ）")
                            if new_count > 0:
                                st.rerun()

        # Manual add
        st.markdown("---")
        with st.expander("➕ 手動で商品を追加", expanded=False):
            ma_col1, ma_col2, ma_col3 = st.columns([2, 3, 2])
            with ma_col1:
                new_asin = st.text_input("ASIN", key="new_master_asin")
            with ma_col2:
                new_pname = st.text_input("商品名（参考）", key="new_master_pname")
            with ma_col3:
                new_pcat = st.selectbox("勘定科目", EXPENSE_CATEGORIES, key="new_master_cat")
            if st.button("追加", type="primary", key="btn_add_master"):
                if new_asin.strip():
                    upsert_product_master(new_asin.strip(), new_pname.strip(), "", new_pcat)
                    st.success(f"✅ {new_asin.strip()} → {new_pcat}")
                    st.rerun()
                else:
                    st.error("ASINを入力してください。")

    # ─── User Management (admin only) ──────────────────────────
    if is_admin:
        with tab_users:
            st.subheader("ユーザー管理")

            users = get_all_users()
            if users:
                st.markdown("#### 登録ユーザー一覧")
                user_data = []
                for u in users:
                    role_label = "管理者" if u["role"] == "admin" else "店舗マネージャー"
                    user_data.append({
                        "ID": u["id"],
                        "ユーザー名": u["username"],
                        "表示名": u.get("display_name", ""),
                        "権限": role_label,
                        "担当店舗": u.get("store_name", ""),
                    })
                st.dataframe(pd.DataFrame(user_data), use_container_width=True, hide_index=True)

                # Delete user
                st.markdown("---")
                st.markdown("#### ユーザー削除")
                del_options = [f"{u['id']} — {u['username']} ({u.get('display_name', '')})" for u in users if u["username"] != "admin"]
                if del_options:
                    del_selected = st.selectbox("削除するユーザー", del_options, key="del_user_select")
                    if st.button("🗑 削除", type="secondary", key="btn_del_user"):
                        del_id = int(del_selected.split(" — ")[0])
                        if delete_user(del_id):
                            st.success("✅ ユーザーを削除しました")
                            st.rerun()
                        else:
                            st.error("管理者ユーザーは削除できません")
                else:
                    st.info("削除可能なユーザーはありません（管理者は削除不可）")

            # Add store manager
            st.markdown("---")
            st.markdown("#### 店舗マネージャー追加")
            um_col1, um_col2 = st.columns(2)
            with um_col1:
                new_um_username = st.text_input("ユーザー名", key="new_um_username")
                new_um_password = st.text_input("パスワード", type="password", key="new_um_password")
            with um_col2:
                new_um_display = st.text_input("表示名", key="new_um_display")
                new_um_store = st.selectbox("担当店舗", STORES, key="new_um_store")

            if st.button("追加", type="primary", key="btn_add_user"):
                if new_um_username.strip() and new_um_password.strip():
                    ok = create_user(
                        new_um_username.strip(),
                        new_um_password.strip(),
                        "store_manager",
                        new_um_store,
                        new_um_display.strip() or new_um_username.strip(),
                    )
                    if ok:
                        st.success(f"✅ ユーザー「{new_um_username.strip()}」を追加しました")
                        st.rerun()
                    else:
                        st.error("このユーザー名は既に使用されています")
                else:
                    st.error("ユーザー名とパスワードを入力してください")
