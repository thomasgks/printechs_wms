# -*- coding: utf-8 -*-
"""Batched / background posting for large WMS Cycle Count Batches."""
from __future__ import annotations

import frappe
from frappe import _
from frappe.utils import cint, flt, now_datetime, nowdate, nowtime, cstr

from printechs_wms.api.cycle_count_batch import (
    API_VERSION,
    BATCH_DT,
    RESULT_DT,
    STOCK_BAL_DT,
    TASK_DT,
    _aggregate_item_totals_from_batch,
    _build_wms_count_map,
    _default_valuation_rate,
    _ensure_account_exists,
    _meta_has,
    _normalize_count_mode,
    _pick_difference_account,
    _tasks_for_batch_processing,
)

BATCH_POST_CHUNK_SIZE = 500
BATCH_POST_ASYNC_THRESHOLD = 2000
BATCH_POST_LOCK_TTL = 7200
LEDGER_DT = "WMS Stock Ledger Entry"


def _batch_post_lock_key(batch_name: str) -> str:
    return f"wms_ccb_post:{batch_name}"


def _acquire_batch_post_lock(batch_name: str) -> None:
    key = _batch_post_lock_key(batch_name)
    if frappe.cache().get_value(key):
        frappe.throw(
            _("Batch posting is already running for {0}. Please wait and refresh — do not click Post again.").format(
                batch_name
            )
        )
    frappe.cache().set_value(key, frappe.session.user or "system", expires_in_sec=BATCH_POST_LOCK_TTL)


def _release_batch_post_lock(batch_name: str) -> None:
    frappe.cache().delete_value(_batch_post_lock_key(batch_name))


def _estimate_wms_key_count(task_names: list[str]) -> int:
    if not task_names:
        return 0
    return cint(
        frappe.db.sql(
            """
            SELECT COUNT(*) FROM (
                SELECT DISTINCT r.item_code, r.bin_location, IFNULL(r.carton_id, '')
                FROM `tabWMS Cycle Count Result` r
                WHERE r.parent IN %(parents)s
                  AND IFNULL(r.item_code, '') != ''
                  AND IFNULL(r.bin_location, '') != ''
            ) x
            """,
            {"parents": task_names},
        )[0][0]
    )


def _load_warehouse_balance_index(company: str, warehouse: str, item_codes: set[str]) -> dict:
    if not item_codes:
        return {}
    rows = frappe.get_all(
        STOCK_BAL_DT,
        filters={"company": company, "warehouse": warehouse, "item_code": ["in", list(item_codes)]},
        fields=["name", "item_code", "location", "carton", "qty"],
        limit_page_length=500000,
    )
    index = {}
    for row in rows:
        key = (
            cstr(row.get("item_code")).strip(),
            cstr(row.get("location")).strip(),
            cstr(row.get("carton") or "").strip(),
        )
        index[key] = {"name": row.name, "qty": flt(row.get("qty"))}
    return index


def _zero_stale_wms_cartons_fast(
    company: str,
    warehouse: str,
    wms_group: dict,
    nowdt,
    batch_name: str | None,
    reconciliation_items: set | None,
    adhoc_location_items: set | None,
) -> tuple[int, list[dict]]:
    if not wms_group:
        return 0, []

    counted_keys = set(wms_group.keys())
    reconciliation_items = reconciliation_items or set()
    adhoc_location_items = adhoc_location_items or set()
    if not reconciliation_items and not adhoc_location_items:
        return 0, []

    rows = frappe.get_all(
        STOCK_BAL_DT,
        filters={"company": company, "warehouse": warehouse, "qty": [">", 0]},
        fields=["name", "item_code", "location", "carton", "qty"],
        limit_page_length=500000,
    )

    cleared: list[dict] = []
    balance_updates: dict = {}
    ledger_rows: list[tuple] = []
    user = frappe.session.user or "Administrator"
    now_str = str(nowdt)
    has_last_txn = _meta_has(STOCK_BAL_DT, "last_txn_datetime")
    has_ledger = frappe.db.exists("DocType", LEDGER_DT)
    has_ledger_wh = _meta_has(LEDGER_DT, "warehouse") if has_ledger else False
    batch_part = frappe.scrub(batch_name or "batch")

    for row in rows:
        item_code = cstr(row.get("item_code")).strip()
        location = cstr(row.get("location")).strip()
        carton = cstr(row.get("carton") or "").strip()
        key = (item_code, location, carton)
        prev_qty = flt(row.get("qty"))
        if prev_qty == 0:
            continue

        should_clear = False
        if item_code in reconciliation_items and key not in counted_keys:
            should_clear = True
        elif (item_code, location) in adhoc_location_items and item_code not in reconciliation_items:
            if key not in counted_keys:
                should_clear = True
        if not should_clear:
            continue

        upd = {"qty": 0}
        if has_last_txn:
            upd["last_txn_datetime"] = nowdt
        balance_updates[row.name] = upd
        cleared.append(
            {
                "item_code": item_code,
                "location": location,
                "carton": carton,
                "previous_qty": prev_qty,
                "qty_after": 0,
            }
        )

        if has_ledger:
            loc_part = frappe.scrub(location or "loc")
            carton_part = frappe.scrub(carton or "none")
            wms_txn_id = f"CCB-CLEAR-{batch_part}-{item_code}-{loc_part}-{carton_part}"
            ledger_row = [
                frappe.generate_hash(length=10),
                user,
                now_str,
                now_str,
                user,
                0,
                0,
                nowdt,
                item_code,
                carton or None,
                -prev_qty,
                batch_name,
                wms_txn_id,
                company,
                location,
                "CycleCount",
                BATCH_DT,
                0,
                _("Cycle count: cleared stale carton not in count"),
            ]
            if has_ledger_wh:
                ledger_row.append(warehouse)
            ledger_rows.append(tuple(ledger_row))

    if balance_updates:
        frappe.db.bulk_update(STOCK_BAL_DT, balance_updates, update_modified=False, chunk_size=BATCH_POST_CHUNK_SIZE)
    if ledger_rows:
        fields = [
            "name",
            "owner",
            "creation",
            "modified",
            "modified_by",
            "docstatus",
            "idx",
            "posting_datetime",
            "item_code",
            "carton",
            "qty_change",
            "voucher_name",
            "wms_txn_id",
            "company",
            "location",
            "event_type",
            "voucher_doctype",
            "qty_after",
            "remarks",
        ]
        if has_ledger_wh:
            fields.append("warehouse")
        frappe.db.bulk_insert(LEDGER_DT, fields, ledger_rows, ignore_duplicates=True, chunk_size=BATCH_POST_CHUNK_SIZE)

    return len(cleared), cleared


def _validate_items_exist(item_codes: set[str]) -> None:
    if not item_codes:
        return
    found = set(
        frappe.get_all("Item", filters={"name": ["in", list(item_codes)]}, pluck="name", limit_page_length=500000)
    )
    missing = sorted(item_codes - found)
    if missing:
        frappe.throw(_("Item not found: {0}").format(missing[0]))


def _apply_wms_balance_chunk(
    company: str,
    warehouse: str,
    batch_name: str,
    chunk: list[tuple],
    balance_index: dict,
    nowdt,
    user: str,
) -> int:
    if not chunk:
        return 0

    has_last_txn = _meta_has(STOCK_BAL_DT, "last_txn_datetime")
    has_ledger = frappe.db.exists("DocType", LEDGER_DT)
    has_ledger_wh = _meta_has(LEDGER_DT, "warehouse") if has_ledger else False
    now_str = str(nowdt)
    batch_part = frappe.scrub(batch_name or "batch")

    balance_updates: dict = {}
    balance_inserts: list[tuple] = []
    ledger_rows: list[tuple] = []
    insert_fields = [
        "name",
        "owner",
        "creation",
        "modified",
        "modified_by",
        "docstatus",
        "idx",
        "company",
        "warehouse",
        "item_code",
        "location",
        "carton",
        "qty",
        "reserved_qty",
    ]
    if has_last_txn:
        insert_fields.append("last_txn_datetime")

    for (item_code, location, carton_key), counted_qty in chunk:
        carton = carton_key or None
        key = (item_code, location, carton_key or "")
        existing = balance_index.get(key)
        previous_qty = flt(existing.get("qty")) if existing else 0.0
        new_qty = float(counted_qty)

        if existing:
            upd = {"qty": new_qty}
            if has_last_txn:
                upd["last_txn_datetime"] = nowdt
            balance_updates[existing["name"]] = upd
        else:
            row = [
                frappe.generate_hash(length=10),
                user,
                now_str,
                now_str,
                user,
                0,
                0,
                company,
                warehouse,
                item_code,
                location,
                carton,
                new_qty,
                0,
            ]
            if has_last_txn:
                row.append(nowdt)
            balance_inserts.append(tuple(row))
            balance_index[key] = {"name": row[0], "qty": new_qty}

        qty_change = new_qty - previous_qty
        if has_ledger and abs(qty_change) >= 1e-9:
            loc_part = frappe.scrub(location or "loc")
            carton_part = frappe.scrub(carton_key or "none")
            wms_txn_id = f"CCB-SET-{batch_part}-{item_code}-{loc_part}-{carton_part}"
            ledger_row = [
                frappe.generate_hash(length=10),
                user,
                now_str,
                now_str,
                user,
                0,
                0,
                nowdt,
                item_code,
                carton,
                qty_change,
                batch_name,
                wms_txn_id,
                company,
                location,
                "CycleCount",
                BATCH_DT,
                new_qty,
                _("Cycle count: set counted balance"),
            ]
            if has_ledger_wh:
                ledger_row.append(warehouse)
            ledger_rows.append(tuple(ledger_row))

    if balance_updates:
        frappe.db.bulk_update(STOCK_BAL_DT, balance_updates, update_modified=False, chunk_size=BATCH_POST_CHUNK_SIZE)
    if balance_inserts:
        frappe.db.bulk_insert(STOCK_BAL_DT, insert_fields, balance_inserts, chunk_size=BATCH_POST_CHUNK_SIZE)
    if ledger_rows:
        ledger_fields = [
            "name",
            "owner",
            "creation",
            "modified",
            "modified_by",
            "docstatus",
            "idx",
            "posting_datetime",
            "item_code",
            "carton",
            "qty_change",
            "voucher_name",
            "wms_txn_id",
            "company",
            "location",
            "event_type",
            "voucher_doctype",
            "qty_after",
            "remarks",
        ]
        if has_ledger_wh:
            ledger_fields.append("warehouse")
        frappe.db.bulk_insert(LEDGER_DT, ledger_fields, ledger_rows, ignore_duplicates=True, chunk_size=BATCH_POST_CHUNK_SIZE)

    return len(chunk)


def _mark_tasks_posted(task_names: list[str]) -> None:
    if not task_names:
        return
    sets = []
    if _meta_has(TASK_DT, "status"):
        sets.append("status = 'Posted'")
    if _meta_has(TASK_DT, "sync_stage"):
        sets.append("sync_stage = 'Posted'")
    if not sets:
        return
    set_clause = ", ".join(sets)
    for i in range(0, len(task_names), 1000):
        chunk = task_names[i : i + 1000]
        frappe.db.sql(
            f"UPDATE `tab{TASK_DT}` SET {set_clause} WHERE name IN %(names)s",
            {"names": chunk},
        )


def _resolve_stock_reconciliation(
    b,
    batch_name: str,
    is_opening: int,
    create_stock_reconciliation: int,
    res_rows,
    company: str,
    warehouse: str,
    posting_date,
    posting_time,
) -> tuple[str | None, str | None]:
    sr_name = None
    sr_note = None
    opening_sr_field = "opening_stock_reconciliation"
    opening_sr_name = b.get(opening_sr_field) if _meta_has(BATCH_DT, opening_sr_field) else None

    existing_batch_sr = None
    if _meta_has(BATCH_DT, "stock_reconciliation"):
        existing_batch_sr = (b.get("stock_reconciliation") or "").strip() or None
        if existing_batch_sr and not frappe.db.exists("Stock Reconciliation", existing_batch_sr):
            existing_batch_sr = None

    if is_opening:
        sr_name = opening_sr_name
        sr_note = "Opening mode: SR not created here; linked API4 Opening SR."
        if _meta_has(BATCH_DT, "stock_reconciliation") and not existing_batch_sr and sr_name:
            b.stock_reconciliation = sr_name
        return sr_name, sr_note

    if existing_batch_sr:
        return existing_batch_sr, "Adjustment SR already linked on batch; reused (no duplicate)."

    if not create_stock_reconciliation:
        return None, "Adjustment mode: create_stock_reconciliation=0; SR not created."

    erp_group = {}
    for r in res_rows:
        item_code = (r.get("item_code") or "").strip()
        qty = float(r.get("counted_qty") or 0)
        if not item_code:
            continue
        erp_group[item_code] = erp_group.get(item_code, 0.0) + qty

    SR_DT = "Stock Reconciliation"
    SR_ITEM_DT = "Stock Reconciliation Item"
    sr_meta = frappe.get_meta(SR_DT)
    sri_meta = frappe.get_meta(SR_ITEM_DT)
    qty_field = "qty" if sri_meta.has_field("qty") else ("quantity" if sri_meta.has_field("quantity") else None)
    if not qty_field:
        frappe.throw(_("Stock Reconciliation Item missing qty field"))
    val_field = (
        "valuation_rate"
        if sri_meta.has_field("valuation_rate")
        else ("rate" if sri_meta.has_field("rate") else None)
    )
    diff_acc = _pick_difference_account(opening_entry=0, company=company)
    _ensure_account_exists(diff_acc)
    sr = frappe.get_doc({"doctype": SR_DT})
    if sr_meta.has_field("company"):
        sr.company = company
    if sr_meta.has_field("posting_date"):
        sr.posting_date = posting_date
    if sr_meta.has_field("posting_time"):
        sr.posting_time = posting_time
    if sr_meta.has_field("purpose"):
        df = sr_meta.get_field("purpose")
        opts = [x.strip() for x in (df.options or "").split("\n") if x.strip()]
        preferred = "Stock Reconciliation"
        sr.purpose = preferred if preferred in opts else (opts[0] if opts else preferred)
    if sr_meta.has_field("set_warehouse"):
        sr.set_warehouse = warehouse
    if sr_meta.has_field("expense_account"):
        sr.expense_account = diff_acc
    if sr_meta.has_field("difference_account"):
        sr.difference_account = diff_acc
    sr.set("items", [])
    item_totals = _aggregate_item_totals_from_batch(batch_name)
    for item_code, counted_qty in erp_group.items():
        previous_qty = flt((item_totals.get(item_code) or {}).get("previous_qty"))
        vr = flt(_default_valuation_rate(item_code, warehouse, previous_qty))
        if flt(vr) <= 0:
            frappe.throw(
                _(
                    "Valuation missing for Item {0} in Warehouse {1}. "
                    "Export verification Excel, fill valuation_rate, and upload before posting."
                ).format(item_code, warehouse)
            )
        row = {"item_code": item_code, "warehouse": warehouse, qty_field: float(counted_qty)}
        if val_field:
            row[val_field] = vr
        sr.append("items", row)
    try:
        sr.insert(ignore_permissions=True)
        sr.submit()
        sr_name = sr.name
        sr_note = "Adjustment SR created."
        if _meta_has(BATCH_DT, "stock_reconciliation"):
            b.stock_reconciliation = sr_name
    except Exception as e:
        msg = str(e) or ""
        if "None of the items have any change in quantity or value" in msg or "None of the items have any change" in msg:
            return None, "Adjustment mode: ERPNext detected no change; SR skipped."
        raise
    return sr_name, sr_note


def execute_confirm_and_post_batch(
    batch_name: str,
    create_stock_reconciliation: int = 0,
    is_opening: int = 0,
    posted_by: str | None = None,
) -> dict:
    create_stock_reconciliation = cint(create_stock_reconciliation or 0)
    is_opening = cint(is_opening or 0)
    posted_by = posted_by or frappe.session.user

    b = frappe.get_doc(BATCH_DT, batch_name)
    status = (b.get("status") or "").strip()
    if status == "Posted":
        existing_sr = (b.get("stock_reconciliation") or "").strip() if _meta_has(BATCH_DT, "stock_reconciliation") else ""
        return {
            "ok": True,
            "api_version": API_VERSION,
            "batch": batch_name,
            "mode": "opening" if is_opening else "adjustment",
            "message": "Batch already posted. No action taken.",
            "updated_balances": 0,
            "sr": existing_sr or None,
        }
    if status not in {"Draft", "Previewed", ""}:
        frappe.throw(_("Batch is not in allowed status. Current status: {0}").format(status))

    company = (b.get("company") or "").strip()
    warehouse = (b.get("warehouse") or "").strip()
    posting_date = b.get("posting_date") or nowdate()
    posting_time = nowtime()
    if not company:
        frappe.throw(_("Batch.company is required"))
    if not warehouse:
        frappe.throw(_("Batch.warehouse is required"))
    if is_opening:
        opening_sr_field = "opening_stock_reconciliation"
        if not (b.get(opening_sr_field) if _meta_has(BATCH_DT, opening_sr_field) else None):
            frappe.throw(_("Opening mode: Upload valuation Excel and create Opening Stock SR first (API 4)."))

    nowdt = now_datetime()
    task_names = _tasks_for_batch_processing(batch_name)
    if not task_names:
        frappe.throw(_("No tasks selected for posting on this batch (check Include in Post and task status)."))

    has_carton_id = _meta_has(RESULT_DT, "carton_id")
    res_fields = ["name", "parent", "item_code", "bin_location", "counted_qty"]
    if has_carton_id:
        res_fields.append("carton_id")
    res_rows = frappe.get_all(
        RESULT_DT,
        filters={"parent": ["in", task_names]},
        fields=res_fields,
        limit_page_length=200000,
    )
    if not res_rows:
        frappe.throw(_("No result lines found for tasks in this batch."))

    task_modes = {}
    if _meta_has(TASK_DT, "count_mode"):
        for t in frappe.get_all(TASK_DT, filters={"name": ["in", task_names]}, fields=["name", "count_mode"]):
            task_modes[t.name] = _normalize_count_mode(t.get("count_mode"))

    reconciliation_items: set[str] = set()
    adhoc_location_items: set[tuple[str, str]] = set()
    for r in res_rows:
        parent = r.get("parent")
        mode = task_modes.get(parent, "reconciliation")
        item_code = (r.get("item_code") or "").strip()
        location = (r.get("bin_location") or "").strip()
        if not item_code or not location:
            continue
        if mode == "reconciliation":
            reconciliation_items.add(item_code)
        else:
            adhoc_location_items.add((item_code, location))

    wms_group = _build_wms_count_map(res_rows, has_carton_id)
    item_codes = {k[0] for k in wms_group}
    _validate_items_exist(item_codes)

    cleared_stale_cartons, cleared_cartons = _zero_stale_wms_cartons_fast(
        company,
        warehouse,
        wms_group,
        nowdt,
        batch_name=batch_name,
        reconciliation_items=reconciliation_items,
        adhoc_location_items=adhoc_location_items,
    )
    frappe.db.commit()

    balance_index = _load_warehouse_balance_index(company, warehouse, item_codes)
    user = posted_by or "Administrator"
    wms_items = list(wms_group.items())
    updated_balances = 0
    for i in range(0, len(wms_items), BATCH_POST_CHUNK_SIZE):
        chunk = wms_items[i : i + BATCH_POST_CHUNK_SIZE]
        updated_balances += _apply_wms_balance_chunk(
            company, warehouse, batch_name, chunk, balance_index, nowdt, user
        )
        frappe.db.commit()

    sr_name, sr_note = _resolve_stock_reconciliation(
        b,
        batch_name,
        is_opening,
        create_stock_reconciliation,
        res_rows,
        company,
        warehouse,
        posting_date,
        posting_time,
    )

    _mark_tasks_posted(task_names)
    if _meta_has(BATCH_DT, "status"):
        pending = frappe.db.count(TASK_DT, {"batch": batch_name, "status": ["!=", "Posted"]})
        b.status = "Previewed" if pending else "Posted"
    if _meta_has(BATCH_DT, "posted_on"):
        b.posted_on = nowdt
    if _meta_has(BATCH_DT, "posted_by"):
        b.posted_by = posted_by
    b.save(ignore_permissions=True)
    frappe.db.commit()

    return {
        "ok": True,
        "api_version": API_VERSION,
        "batch": batch_name,
        "mode": "opening" if is_opening else "adjustment",
        "updated_balances": updated_balances,
        "cleared_stale_cartons": cleared_stale_cartons,
        "cleared_cartons": cleared_cartons[:20],
        "sr": sr_name,
        "sr_note": sr_note,
        "difference_account_opening": _pick_difference_account(opening_entry=1, company=company),
        "difference_account_adjustment": _pick_difference_account(opening_entry=0, company=company),
    }


def post_cycle_count_batch_job(
    batch_name: str,
    create_stock_reconciliation: int = 0,
    is_opening: int = 0,
    posted_by: str | None = None,
):
    frappe.set_user(posted_by or "Administrator")
    try:
        return execute_confirm_and_post_batch(
            batch_name,
            create_stock_reconciliation=create_stock_reconciliation,
            is_opening=is_opening,
            posted_by=posted_by,
        )
    finally:
        _release_batch_post_lock(batch_name)


@frappe.whitelist()
def get_batch_post_status(batch_name: str):
    if not batch_name:
        frappe.throw(_("batch_name is required"))
    locked = bool(frappe.cache().get_value(_batch_post_lock_key(batch_name)))
    status = frappe.db.get_value(BATCH_DT, batch_name, ["status", "posted_on", "stock_reconciliation"], as_dict=True)
    return {
        "ok": True,
        "batch": batch_name,
        "posting_in_progress": locked,
        "status": (status or {}).get("status"),
        "posted_on": (status or {}).get("posted_on"),
        "stock_reconciliation": (status or {}).get("stock_reconciliation"),
    }


def queue_or_run_confirm_and_post_batch(
    batch_name: str,
    create_stock_reconciliation: int = 0,
    is_opening: int = 0,
    force_sync: int = 0,
) -> dict:
    create_stock_reconciliation = cint(create_stock_reconciliation or 0)
    is_opening = cint(is_opening or 0)
    force_sync = cint(force_sync or 0)
    posted_by = frappe.session.user

    b = frappe.get_doc(BATCH_DT, batch_name)
    status = (b.get("status") or "").strip()
    if status == "Posted":
        existing_sr = (b.get("stock_reconciliation") or "").strip() if _meta_has(BATCH_DT, "stock_reconciliation") else ""
        return {
            "ok": True,
            "api_version": API_VERSION,
            "batch": batch_name,
            "mode": "opening" if is_opening else "adjustment",
            "message": "Batch already posted. No action taken.",
            "updated_balances": 0,
            "sr": existing_sr or None,
        }
    if status not in {"Draft", "Previewed", ""}:
        frappe.throw(_("Batch is not in allowed status. Current status: {0}").format(status))

    task_names = _tasks_for_batch_processing(batch_name)
    if not task_names:
        frappe.throw(_("No tasks selected for posting on this batch (check Include in Post and task status)."))

    key_count = _estimate_wms_key_count(task_names)
    use_background = (not force_sync) and key_count >= BATCH_POST_ASYNC_THRESHOLD

    _acquire_batch_post_lock(batch_name)

    if use_background:
        frappe.enqueue(
            method="printechs_wms.api.cycle_count_batch_post.post_cycle_count_batch_job",
            queue="long",
            timeout=BATCH_POST_LOCK_TTL,
            job_name=f"wms_ccb_post_{batch_name}",
            batch_name=batch_name,
            create_stock_reconciliation=create_stock_reconciliation,
            is_opening=is_opening,
            posted_by=posted_by,
            now=frappe.flags.in_test,
        )
        return {
            "ok": True,
            "api_version": API_VERSION,
            "batch": batch_name,
            "queued": True,
            "wms_keys": key_count,
            "message": _(
                "Posting started in background for {0} location rows. Refresh this page in a few minutes."
            ).format(key_count),
        }

    try:
        result = execute_confirm_and_post_batch(
            batch_name,
            create_stock_reconciliation=create_stock_reconciliation,
            is_opening=is_opening,
            posted_by=posted_by,
        )
        result["queued"] = False
        return result
    finally:
        _release_batch_post_lock(batch_name)
