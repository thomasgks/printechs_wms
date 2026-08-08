# -*- coding: utf-8 -*-
"""
cycle_count_batch.py

APIs included:
1) sync_task_capture_only(payload)              -> capture task + replace results safely (no duplicates)
2) load_actual_stock_preview(batch_name)        -> compute system_qty/delta + fill Batch Summary
3) export_opening_valuation_template(batch_name)-> export item-level verification Excel for finance
4) upload_opening_valuation_file(...)           -> read excel + create Stock Reconciliation (adjustment)
5) confirm_and_post_batch(...)                  -> update WMS Stock Balance + link uploaded SR

Notes:
- FIXED: API4 file path indentation bug (file always resolves)
- FIXED: API4 avoids ERPNext popup "None of the items..." by pre-checking diffs before creating SR
- SAFE: Detects SR item fields dynamically across ERPNext versions
"""

from __future__ import annotations

import frappe
from frappe import _
from frappe.utils import (
    today,
    now_datetime,
    nowdate,
    nowtime,
    cint,
    flt,
    cstr,
)

# ---------------------------------------------------------------------
# DOCTYPES
# ---------------------------------------------------------------------
TASK_DT = "WMS Cycle Count Task"
RESULT_DT = "WMS Cycle Count Result"
BATCH_DT = "WMS Cycle Count Batch"
SUMMARY_CHILD_DT = "WMS Cycle Count Batch Summary"
STOCK_BAL_DT = "WMS Stock Balance"

API_VERSION = "cycle_count_batch_v9_type_safe_all"

VERIFICATION_SHEET = "Cycle Count Verification"
LEGACY_SHEET = "Opening Valuation Upload"


# ---------------------------------------------------------------------
# CONFIG: Difference Accounts (fallback when Company fields are empty)
# ---------------------------------------------------------------------
OPENING_DIFFERENCE_ACCOUNT = "1.04.01.01 - Temporary Opening - MAATC"     # MUST be Asset/Liability
ADJUSTMENT_DIFFERENCE_ACCOUNT = "5.01.01.01 - Stock Adjustment - MAATC"   # legacy fallback only


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def _get_meta(dt: str):
    return frappe.get_meta(dt)


def _meta_has(dt: str, fieldname: str) -> bool:
    try:
        return _get_meta(dt).has_field(fieldname)
    except Exception:
        return False


def _safe_float(v) -> float:
    try:
        return float(v or 0)
    except Exception:
        return 0.0


def _row_name(value) -> str:
    """Normalize child-row names for safe string comparison (PyMySQL may return int)."""
    return cstr(value or "")


def _is_later_row_name(current, existing) -> bool:
    return _row_name(current) > _row_name(existing)


def _normalize_task_status(in_status: str | None) -> str:
    allowed = {"Open", "In Progress", "Completed", "Validated", "Posted", "Closed", "Cancelled"}
    if not in_status:
        return "Completed"
    s = str(in_status).strip()
    if s in allowed:
        return s
    mapping = {"RECEIVED": "Completed", "DONE": "Completed", "FINISHED": "Completed"}
    return mapping.get(s.upper(), "Completed")


def _task_by_external_ref(external_ref: str):
    if not external_ref:
        return None
    return frappe.db.get_value(TASK_DT, {"external_ref": external_ref}, "name")


def _extract_carton_id(row: dict) -> str | None:
    v = (
        row.get("carton_id")
        or row.get("carton")
        or row.get("ctn_id")
        or row.get("carton_no")
        or row.get("carton_ref")
    )
    if v is None:
        return None
    v = str(v).strip()
    return v or None


def get_or_create_batch(company: str, warehouse: str, warehouse_code: str, posting_date: str | None):
    """
    One open Draft batch per warehouse cycle (posting_date on tasks may differ by day).
    """
    bname = frappe.db.get_value(
        BATCH_DT,
        {
            "company": company,
            "warehouse": warehouse,
            "warehouse_code": warehouse_code,
            "status": ["in", ["Draft", "Previewed"]],
        },
        "name",
        order_by="modified desc",
    )
    if bname:
        return bname

    b = frappe.get_doc(
        {
            "doctype": BATCH_DT,
            "company": company,
            "warehouse": warehouse,
            "warehouse_code": warehouse_code,
            "posting_date": posting_date or today(),
            "status": "Draft",
        }
    )
    b.insert(ignore_permissions=True)
    return b.name


def _tasks_for_batch_processing(batch_name: str) -> list[str]:
    """
    Tasks included in Preview/Post: linked to batch, include_in_post=1, not already Posted.
    """
    rows = frappe.get_all(
        TASK_DT,
        filters={"batch": batch_name},
        fields=["name", "status"],
        limit_page_length=200000,
    )
    names = []
    for row in rows:
        if (row.get("status") or "").strip() == "Posted":
            continue
        if _meta_has(TASK_DT, "include_in_post"):
            if not cint(frappe.db.get_value(TASK_DT, row.name, "include_in_post")):
                continue
        names.append(row.name)
    return names


@frappe.whitelist()
def get_batch_linked_tasks(batch_name: str):
    """List tasks linked to a batch for the batch form task-selection panel."""
    if not batch_name:
        frappe.throw(_("batch_name is required"))

    if not frappe.db.exists(BATCH_DT, batch_name):
        frappe.throw(_("Batch {0} not found.").format(batch_name))

    fields = [
        "name",
        "posting_date",
        "status",
        "external_ref",
        "modified",
        "creation",
    ]
    if _meta_has(TASK_DT, "count_mode"):
        fields.append("count_mode")
    if _meta_has(TASK_DT, "include_in_post"):
        fields.append("include_in_post")
    if _meta_has(TASK_DT, "sync_stage"):
        fields.append("sync_stage")

    tasks = frappe.get_all(
        TASK_DT,
        filters={"batch": batch_name},
        fields=fields,
        order_by="posting_date asc, creation asc",
        limit_page_length=200000,
    )

    included_count = 0
    pending_count = 0
    for row in tasks:
        row["line_count"] = frappe.db.count(RESULT_DT, {"parent": row.name})
        row["include_in_post"] = cint(row.get("include_in_post", 1))
        is_posted = (row.get("status") or "").strip() == "Posted"
        if not is_posted:
            pending_count += 1
            if row["include_in_post"]:
                included_count += 1

    batch_status = frappe.db.get_value(BATCH_DT, batch_name, "status")

    return {
        "ok": True,
        "batch": batch_name,
        "batch_status": batch_status,
        "tasks": tasks,
        "total": len(tasks),
        "included_count": included_count,
        "pending_count": pending_count,
    }


@frappe.whitelist()
def set_batch_task_inclusion(batch_name: str, selections=None):
    """Update include_in_post for one or more tasks on a batch."""
    if not batch_name:
        frappe.throw(_("batch_name is required"))

    selections = frappe.parse_json(selections) if isinstance(selections, str) else (selections or [])
    if not isinstance(selections, list):
        frappe.throw(_("selections must be a list"))

    batch_status = frappe.db.get_value(BATCH_DT, batch_name, "status")
    if batch_status == "Posted":
        frappe.throw(_("Cannot change task selection on a Posted batch."))

    if not _meta_has(TASK_DT, "include_in_post"):
        frappe.throw(_("Include in Post field is not available on WMS Cycle Count Task."))

    updated = 0
    for row in selections:
        if not isinstance(row, dict):
            continue
        task_name = (row.get("name") or row.get("task") or "").strip()
        if not task_name:
            continue
        if frappe.db.get_value(TASK_DT, task_name, "batch") != batch_name:
            continue
        if (frappe.db.get_value(TASK_DT, task_name, "status") or "").strip() == "Posted":
            continue
        include = cint(row.get("include_in_post", 1))
        frappe.db.set_value(TASK_DT, task_name, "include_in_post", include, update_modified=True)
        updated += 1

    batch_reset = False
    if updated and batch_status == "Previewed":
        frappe.db.set_value(BATCH_DT, batch_name, "status", "Draft", update_modified=True)
        if _meta_has(BATCH_DT, "preview_loaded_on"):
            frappe.db.set_value(BATCH_DT, batch_name, "preview_loaded_on", None, update_modified=False)
        batch_reset = True

    summary = get_batch_linked_tasks(batch_name)
    summary["updated"] = updated
    summary["batch_reset"] = batch_reset
    return summary


def link_task_to_batch(task_doc):
    if task_doc.get("batch"):
        return task_doc.batch

    bname = get_or_create_batch(
        task_doc.company,
        task_doc.warehouse,
        task_doc.warehouse_code,
        getattr(task_doc, "posting_date", None),
    )
    task_doc.db_set("batch", bname, update_modified=True)

    batch_status = frappe.db.get_value(BATCH_DT, bname, "status")
    if batch_status == "Previewed":
        frappe.db.set_value(BATCH_DT, bname, "status", "Draft", update_modified=True)
        if _meta_has(BATCH_DT, "preview_loaded_on"):
            frappe.db.set_value(BATCH_DT, bname, "preview_loaded_on", None, update_modified=False)

    if _meta_has(TASK_DT, "sync_stage"):
        task_doc.db_set("sync_stage", "In Batch", update_modified=True)

    return bname


def _get_company_stock_adjustment_account(company: str | None) -> str | None:
    company = (company or "").strip()
    if not company or not _meta_has("Company", "stock_adjustment_account"):
        return None
    acc = (frappe.db.get_value("Company", company, "stock_adjustment_account") or "").strip()
    return acc or None


def _get_company_opening_difference_account(company: str | None) -> str | None:
    company = (company or "").strip()
    if not company:
        return None
    if _meta_has("Company", "round_off_for_opening"):
        acc = (frappe.db.get_value("Company", company, "round_off_for_opening") or "").strip()
        if acc:
            return acc
    return None


def _pick_difference_account(opening_entry: int, company: str | None = None) -> str:
    """
    Opening entry requires Asset/Liability account in ERPNext.
    Normal adjustment uses Company.stock_adjustment_account (Stock Settings tab).
    """
    if cint(opening_entry):
        return _get_company_opening_difference_account(company) or OPENING_DIFFERENCE_ACCOUNT

    acc = _get_company_stock_adjustment_account(company)
    if acc:
        return acc
    if ADJUSTMENT_DIFFERENCE_ACCOUNT:
        return ADJUSTMENT_DIFFERENCE_ACCOUNT
    frappe.throw(
        _(
            "Stock Adjustment Account is not set on Company {0}. "
            "Open Company → Stock and Manufacturing → Stock Settings and set it."
        ).format(company or "")
    )


def _ensure_account_exists(account_name: str):
    if not account_name or not cstr(account_name).strip():
        frappe.throw(_("Difference account is required."))
    if not frappe.db.exists("Account", account_name):
        frappe.throw(_("Account not found: {0}").format(account_name))


def _validate_opening_account(diff_acc: str, company: str):
    """
    Opening SR requires Asset/Liability leaf account for same company.
    """
    _ensure_account_exists(diff_acc)

    acc = frappe.db.get_value(
        "Account",
        diff_acc,
        ["root_type", "company", "is_group", "disabled"],
        as_dict=True,
    )
    if not acc:
        frappe.throw(_("Opening diff account not found: {0}").format(diff_acc))
    if acc.company != company:
        frappe.throw(_("Opening diff account company mismatch: {0}").format(diff_acc))
    if cint(acc.is_group or 0) == 1:
        frappe.throw(_("Opening diff account cannot be group: {0}").format(diff_acc))
    if cint(acc.disabled or 0) == 1:
        frappe.throw(_("Opening diff account is disabled: {0}").format(diff_acc))
    if acc.root_type not in ("Asset", "Liability"):
        frappe.throw(
            _("Opening Entry requires Asset/Liability account. {0} root_type={1}").format(diff_acc, acc.root_type)
        )


def _get_stock_snapshot(item_code: str, warehouse: str, posting_date: str, posting_time: str):
    """
    qty + valuation_rate as of posting date/time (best effort across versions).
    Tries erpnext.stock.utils.get_stock_balance; falls back to Bin (current).
    """
    try:
        from erpnext.stock.utils import get_stock_balance  # type: ignore

        qty, rate = get_stock_balance(
            item_code=item_code,
            warehouse=warehouse,
            posting_date=posting_date,
            posting_time=posting_time,
            with_valuation_rate=True,
        )
        return flt(qty), flt(rate)
    except Exception:
        pass

    b = frappe.db.get_value(
        "Bin",
        {"item_code": item_code, "warehouse": warehouse},
        ["actual_qty", "valuation_rate"],
        as_dict=True,
    ) or {}
    return flt(b.get("actual_qty") or 0), flt(b.get("valuation_rate") or 0)


def _get_erp_bin_qty(item_code: str, warehouse: str) -> float:
    return flt(
        frappe.db.get_value(
            "Bin",
            {"item_code": item_code, "warehouse": warehouse},
            "actual_qty",
        )
        or 0
    )


def _default_valuation_rate(item_code: str, warehouse: str, previous_qty: float) -> float:
    """Bin rate when stock exists; otherwise Item master valuation/standard rate."""
    erp_qty = _get_erp_bin_qty(item_code, warehouse)
    ref_qty = flt(previous_qty) if flt(previous_qty) > 0 else flt(erp_qty)
    if flt(ref_qty) > 0:
        bin_rate = frappe.db.get_value(
            "Bin",
            {"item_code": item_code, "warehouse": warehouse},
            "valuation_rate",
        )
        if flt(bin_rate) > 0:
            return flt(bin_rate)

    for field in ("valuation_rate", "standard_rate"):
        item_rate = frappe.db.get_value("Item", item_code, field)
        if flt(item_rate) > 0:
            return flt(item_rate)
    return 0.0


def _normalize_count_mode(mode: str | None) -> str:
    """Reconciliation = replace location truth. Adhoc Add = discover extra stock in new carton."""
    m = cstr(mode).strip().lower().replace(" ", "_").replace("-", "_")
    if m in ("adhoc", "adhoc_add", "add", "additional", "add_stock", "adhocadd"):
        return "adhoc_add"
    return "reconciliation"


def _aggregate_item_totals_from_batch(batch_name: str) -> dict[str, dict]:
    """
    Sum preview quantities at item level (warehouse-wide, no bin in Excel).
    Returns {item_code: {previous_qty, counted_qty, delta_qty}}.

    Adhoc Add tasks add to ERP current qty; Reconciliation tasks set counted truth.
    """
    b = frappe.get_doc(BATCH_DT, batch_name)
    warehouse = b.get("warehouse")
    grouped: dict[str, dict] = {}

    if b.get("summary"):
        task_names = frappe.get_all(TASK_DT, filters={"batch": batch_name}, pluck="name")
        task_modes = {}
        if task_names and _meta_has(TASK_DT, "count_mode"):
            for t in frappe.get_all(
                TASK_DT, filters={"name": ["in", task_names]}, fields=["name", "count_mode"]
            ):
                task_modes[t.name] = _normalize_count_mode(t.get("count_mode"))

        # Map summary rows — batch summary has no task ref; treat as reconciliation
        for row in b.summary:
            item_code = cstr(row.get("item_code")).strip()
            if not item_code:
                continue
            bucket = grouped.setdefault(
                item_code,
                {
                    "previous_qty": 0.0,
                    "counted_qty": 0.0,
                    "reconciliation_counted": 0.0,
                    "adhoc_counted": 0.0,
                    "wms_previous": 0.0,
                },
            )
            bucket["wms_previous"] += flt(row.get("total_system_qty"))
            bucket["reconciliation_counted"] += flt(row.get("total_counted_qty"))
    else:
        task_names = _tasks_for_batch_processing(batch_name)
        if not task_names:
            return grouped

        task_modes = {}
        if _meta_has(TASK_DT, "count_mode"):
            for t in frappe.get_all(
                TASK_DT, filters={"name": ["in", task_names]}, fields=["name", "count_mode"]
            ):
                task_modes[t.name] = _normalize_count_mode(t.get("count_mode"))

        res_fields = ["parent", "item_code", "counted_qty", "system_qty"]
        res_rows = frappe.get_all(
            RESULT_DT,
            filters={"parent": ["in", task_names]},
            fields=res_fields,
            limit_page_length=200000,
        )
        for row in res_rows:
            item_code = cstr(row.get("item_code")).strip()
            if not item_code:
                continue
            mode = task_modes.get(row.get("parent"), "reconciliation")
            bucket = grouped.setdefault(
                item_code,
                {
                    "previous_qty": 0.0,
                    "counted_qty": 0.0,
                    "reconciliation_counted": 0.0,
                    "adhoc_counted": 0.0,
                    "wms_previous": 0.0,
                },
            )
            counted = flt(row.get("counted_qty"))
            bucket["wms_previous"] += flt(row.get("system_qty"))
            if mode == "adhoc_add":
                bucket["adhoc_counted"] += counted
            else:
                bucket["reconciliation_counted"] += counted

    for item_code, data in grouped.items():
        erp_current = _get_erp_bin_qty(item_code, warehouse) if warehouse else 0.0
        if data["adhoc_counted"] and not data["reconciliation_counted"]:
            # Pure adhoc: additional stock discovered
            data["counted_qty"] = erp_current + data["adhoc_counted"]
            data["previous_qty"] = erp_current
        elif data["reconciliation_counted"]:
            data["counted_qty"] = data["reconciliation_counted"] + data["adhoc_counted"]
            data["previous_qty"] = data["wms_previous"] or erp_current
        else:
            data["counted_qty"] = flt(data.get("counted_qty"))
            data["previous_qty"] = data["wms_previous"] or erp_current
        data["delta_qty"] = flt(data["counted_qty"]) - flt(data["previous_qty"])

    return grouped


def _validate_upload_counts_match_batch(batch_name: str, uploaded_by_item: dict[str, float]) -> None:
    """
    Block finance Excel upload when counted qty was changed vs device/batch totals.
    Location-level WMS post uses task lines; SR uses item totals — they must match.
    valuation_rate may still be overridden in Excel.
    """
    if not batch_name:
        return

    if not _batch_preview_loaded(batch_name):
        frappe.throw(
            _("Run 'Load Actual Stock Preview' on the batch before uploading verification Excel.")
        )

    expected = _aggregate_item_totals_from_batch(batch_name)
    if not expected:
        frappe.throw(_("No expected item totals found on batch {0}.").format(batch_name))

    mismatches = []
    missing = []
    extras = []

    for item_code, data in expected.items():
        exp_qty = flt(data.get("counted_qty"))
        if item_code not in uploaded_by_item:
            missing.append((item_code, exp_qty))
            continue
        up_qty = flt(uploaded_by_item[item_code])
        if abs(up_qty - exp_qty) > 1e-6:
            mismatches.append((item_code, exp_qty, up_qty))

    for item_code in uploaded_by_item:
        if item_code not in expected:
            extras.append(item_code)

    if not (mismatches or missing or extras):
        return

    lines = [
        _(
            "Uploaded counted qty must match batch device totals (do not edit counted_qty in Excel). "
            "Change counts on mobile and re-run Preview, or finance may edit valuation_rate only."
        )
    ]
    for item_code, exp_qty, up_qty in mismatches:
        lines.append(
            _("• Item {0}: uploaded {1}, batch total {2}").format(item_code, up_qty, exp_qty)
        )
    for item_code, exp_qty in missing:
        lines.append(_("• Item {0}: missing in Excel (batch total {1})").format(item_code, exp_qty))
    for item_code in extras:
        lines.append(_("• Item {0}: not in this batch").format(item_code))

    frappe.throw("<br>".join(lines), title=_("Count qty mismatch"))


def _batch_preview_loaded(batch_name: str) -> bool:
    status = cstr(frappe.db.get_value(BATCH_DT, batch_name, "status")).strip()
    if status == "Previewed":
        return True
    if _meta_has(BATCH_DT, "preview_loaded_on"):
        return bool(frappe.db.get_value(BATCH_DT, batch_name, "preview_loaded_on"))
    return False


def _build_wms_count_map(res_rows, has_carton_id: bool) -> dict[tuple, float]:
    """One counted qty per (item, location, carton); latest result row wins."""
    grouped: dict[tuple, dict] = {}
    for r in res_rows:
        item_code = (r.get("item_code") or "").strip()
        location = (r.get("bin_location") or "").strip()
        if not item_code or not location:
            continue
        carton = ((r.get("carton_id") if has_carton_id else None) or "").strip()
        key = (item_code, location, carton)
        rname = _row_name(r.get("name"))
        qty = _safe_float(r.get("counted_qty"))
        if key not in grouped or _is_later_row_name(rname, grouped[key].get("name")):
            grouped[key] = {"qty": qty, "name": rname}
    return {k: v["qty"] for k, v in grouped.items()}


def _zero_stale_wms_cartons(
    company: str,
    warehouse: str,
    wms_group: dict,
    nowdt,
    batch_name: str | None = None,
    reconciliation_items: set | None = None,
    adhoc_location_items: set | None = None,
) -> tuple[int, list[dict]]:
    """
    Clear WMS balances not in the cycle count.

    Reconciliation items: warehouse-wide — any carton/location not in wms_group is zeroed
    (matches ERP item-level SR total with WMS sum).

    Adhoc-only items: only at counted locations — other cartons at that location are zeroed;
    other locations are left unchanged.
    """
    if not wms_group:
        return 0, []

    counted_keys = set(wms_group.keys())
    reconciliation_items = reconciliation_items or set()
    adhoc_location_items = adhoc_location_items or set()
    cleared: list[dict] = []
    seen_balance_names: set[str] = set()

    def _clear_row(row, item_code: str, location: str, carton: str) -> None:
        row_name = row.get("name")
        if row_name in seen_balance_names:
            return
        prev_qty = flt(row.get("qty"))
        if prev_qty == 0:
            return
        seen_balance_names.add(row_name)
        frappe.db.set_value(STOCK_BAL_DT, row_name, "qty", 0, update_modified=False)
        if _meta_has(STOCK_BAL_DT, "last_txn_datetime"):
            frappe.db.set_value(
                STOCK_BAL_DT, row_name, "last_txn_datetime", nowdt, update_modified=False
            )
        cleared.append(
            {
                "item_code": item_code,
                "location": location,
                "carton": carton,
                "previous_qty": prev_qty,
                "qty_after": 0,
            }
        )
        _write_cycle_count_ledger_clear(
            company=company,
            warehouse=warehouse,
            batch_name=batch_name,
            item_code=item_code,
            location=location,
            carton=carton,
            previous_qty=prev_qty,
            nowdt=nowdt,
        )

    for item_code in reconciliation_items:
        allowed_keys = {k for k in counted_keys if k[0] == item_code}
        rows = frappe.get_all(
            STOCK_BAL_DT,
            filters={
                "company": company,
                "warehouse": warehouse,
                "item_code": item_code,
            },
            fields=["name", "location", "carton", "qty"],
        )
        for row in rows:
            location = (row.get("location") or "").strip()
            carton = (row.get("carton") or "").strip()
            if (item_code, location, carton) in allowed_keys:
                continue
            _clear_row(row, item_code, location, carton)

    for item_code, location in adhoc_location_items:
        if item_code in reconciliation_items:
            continue
        rows = frappe.get_all(
            STOCK_BAL_DT,
            filters={
                "company": company,
                "warehouse": warehouse,
                "item_code": item_code,
                "location": location,
            },
            fields=["name", "carton", "qty"],
        )
        for row in rows:
            carton = (row.get("carton") or "").strip()
            if (item_code, location, carton) in counted_keys:
                continue
            _clear_row(row, item_code, location, carton)

    return len(cleared), cleared


def _write_cycle_count_ledger_clear(
    company: str,
    warehouse: str,
    batch_name: str | None,
    item_code: str,
    location: str,
    carton: str,
    previous_qty: float,
    nowdt,
) -> None:
    """Ledger row so desktop pull/incremental sync sees stale carton cleared."""
    LEDGER_DT = "WMS Stock Ledger Entry"
    if not frappe.db.exists("DocType", LEDGER_DT):
        return

    batch_part = frappe.scrub(batch_name or "batch")
    loc_part = frappe.scrub(location or "loc")
    carton_part = frappe.scrub(carton or "none")
    wms_txn_id = f"CCB-CLEAR-{batch_part}-{item_code}-{loc_part}-{carton_part}"

    if frappe.db.exists(LEDGER_DT, {"wms_txn_id": wms_txn_id}):
        return

    doc = frappe.get_doc(
        {
            "doctype": LEDGER_DT,
            "posting_datetime": nowdt,
            "company": company,
            "item_code": item_code,
            "location": location,
            "carton": carton or None,
            "qty_change": -flt(previous_qty),
            "qty_after": 0,
            "event_type": "CycleCount",
            "voucher_doctype": BATCH_DT,
            "voucher_name": batch_name,
            "wms_txn_id": wms_txn_id,
            "remarks": _("Cycle count: cleared stale carton not in count"),
        }
    )
    if _meta_has(LEDGER_DT, "warehouse"):
        doc.warehouse = warehouse
    doc.insert(ignore_permissions=True)


def _write_cycle_count_ledger_set(
    company: str,
    warehouse: str,
    batch_name: str | None,
    item_code: str,
    location: str,
    carton: str,
    previous_qty: float,
    new_qty: float,
    nowdt,
) -> None:
    """Ledger row when cycle count sets/adjusts a carton balance (desktop pull sync)."""
    LEDGER_DT = "WMS Stock Ledger Entry"
    if not frappe.db.exists("DocType", LEDGER_DT):
        return

    qty_change = flt(new_qty) - flt(previous_qty)
    if abs(qty_change) < 1e-9:
        return

    batch_part = frappe.scrub(batch_name or "batch")
    loc_part = frappe.scrub(location or "loc")
    carton_part = frappe.scrub(carton or "none")
    wms_txn_id = f"CCB-SET-{batch_part}-{item_code}-{loc_part}-{carton_part}"

    if frappe.db.exists(LEDGER_DT, {"wms_txn_id": wms_txn_id}):
        return

    doc = frappe.get_doc(
        {
            "doctype": LEDGER_DT,
            "posting_datetime": nowdt,
            "company": company,
            "item_code": item_code,
            "location": location,
            "carton": carton or None,
            "qty_change": qty_change,
            "qty_after": flt(new_qty),
            "event_type": "CycleCount",
            "voucher_doctype": BATCH_DT,
            "voucher_name": batch_name,
            "wms_txn_id": wms_txn_id,
            "remarks": _("Cycle count: set counted balance"),
        }
    )
    if _meta_has(LEDGER_DT, "warehouse"):
        doc.warehouse = warehouse
    doc.insert(ignore_permissions=True)


def _company_currency(company: str) -> str:
    return cstr(frappe.db.get_value("Company", company, "default_currency") or "SAR").strip() or "SAR"


# ---------------------------------------------------------------------
# API 1: Desktop -> Capture Task (REPLACE results, NO append)
# ---------------------------------------------------------------------
@frappe.whitelist()
def sync_task_capture_only(payload: dict | None = None):
    """
    Desktop pushes task header + lines.
    - If task exists by external_ref -> replace results fully (delete child rows)
    - If not exists -> create task + results
    """
    if payload is None:
        payload = frappe.local.form_dict.get("payload") or frappe.local.form_dict or {}

    task = payload.get("task") or payload.get("header") or payload.get("task_header") or payload
    lines = payload.get("lines") or payload.get("results") or payload.get("items") or []

    if not isinstance(task, dict):
        frappe.throw(_("Invalid payload.task/header (must be dict)."))

    external_ref = (task.get("external_ref") or task.get("external_task_ref") or task.get("task_id") or "").strip()
    if not external_ref:
        frappe.throw(_("external_ref is required (desktop task id)."))

    company = task.get("company")
    warehouse = task.get("warehouse")
    warehouse_code = task.get("warehouse_code")
    bin_location = (task.get("bin_location") or "").strip()

    if not company:
        frappe.throw(_("company is required."))
    if not warehouse:
        frappe.throw(_("warehouse is required (ERP Warehouse link)."))
    if not warehouse_code:
        frappe.throw(_("warehouse_code is required."))

    posting_date = task.get("posting_date") or today()
    status = _normalize_task_status(task.get("status") or "Completed")
    count_mode = _normalize_count_mode(task.get("count_mode") or task.get("mode") or "Reconciliation")

    existing_name = _task_by_external_ref(external_ref)

    # Resolve child doctype from task's "results" table (fallback RESULT_DT)
    result_dt = RESULT_DT
    try:
        tf = frappe.get_meta(TASK_DT).get_field("results")
        if tf and getattr(tf, "options", None):
            result_dt = tf.options
    except Exception:
        pass

    result_has_carton = _meta_has(result_dt, "carton_id")

    carton_is_link = False
    carton_reqd = False
    if result_has_carton:
        try:
            f = frappe.get_meta(result_dt).get_field("carton_id")
            if f:
                carton_is_link = (getattr(f, "fieldtype", None) == "Link")
                # if Link -> we do not force set (user asked earlier to use Data)
                if not carton_is_link:
                    carton_reqd = getattr(f, "reqd", False)
        except Exception:
            pass

    def _delete_existing_child_rows(parent_name: str):
        # robust delete using table name
        results_field = frappe.get_meta(TASK_DT).get_field("results")
        if not results_field or not getattr(results_field, "options", None):
            return
        child_doctype = results_field.options

        table = None
        try:
            table = getattr(frappe.get_meta(child_doctype), "table_name", None)
        except Exception:
            table = None

        if not table:
            try:
                table = frappe.db.get_table_name(child_doctype)
            except Exception:
                table = None

        if table:
            frappe.db.sql(f"DELETE FROM `{table}` WHERE parent=%s", (parent_name,))

    if existing_name:
        doc = frappe.get_doc(TASK_DT, existing_name)

        # update header
        doc.company = company
        doc.warehouse = warehouse
        doc.warehouse_code = warehouse_code
        if _meta_has(TASK_DT, "bin_location"):
            doc.bin_location = bin_location
        if _meta_has(TASK_DT, "posting_date"):
            doc.posting_date = posting_date
        if _meta_has(TASK_DT, "status"):
            doc.status = status
        if _meta_has(TASK_DT, "sync_stage"):
            doc.sync_stage = "Captured"
        if _meta_has(TASK_DT, "sync_status"):
            doc.sync_status = "Synced"
        if _meta_has(TASK_DT, "count_mode"):
            doc.count_mode = "Adhoc Add" if count_mode == "adhoc_add" else "Reconciliation"
        if _meta_has(TASK_DT, "include_in_post"):
            doc.include_in_post = 1

        # delete all existing child rows to avoid append duplicates
        _delete_existing_child_rows(doc.name)
        frappe.db.commit()
        doc.reload()
        doc.set("results", [])

        # rebuild results
        if isinstance(lines, list):
            for i, row in enumerate(lines, start=1):
                if not isinstance(row, dict):
                    continue
                item_code = row.get("item_code") or row.get("item") or row.get("code")
                if not item_code:
                    continue
                row_bin = row.get("bin_location") or bin_location
                counted_qty = _safe_float(row.get("counted_qty") or row.get("qty") or row.get("counted") or 0)
                carton_id_val = _extract_carton_id(row)

                if result_has_carton and (not carton_is_link) and carton_reqd:
                    if not (carton_id_val and str(carton_id_val).strip()):
                        frappe.throw(_("Row {0}: carton_id is required (Item {1}).").format(i, item_code))

                child_dict = {
                    "item_code": item_code,
                    "bin_location": row_bin,
                    "counted_qty": counted_qty,
                    "system_qty": 0.0,
                    "delta_qty": counted_qty,
                    "has_discrepancy": 1 if abs(counted_qty) > 0.000001 else 0,
                }
                if result_has_carton and (not carton_is_link):
                    child_dict["carton_id"] = (carton_id_val or "").strip()

                doc.append("results", child_dict)

                # some versions need explicit set
                if result_has_carton and (not carton_is_link) and doc.results:
                    doc.results[-1].carton_id = (carton_id_val or "").strip()

        doc.save(ignore_permissions=True)
        updated_lines = len(doc.results)

    else:
        doc = frappe.get_doc(
            {
                "doctype": TASK_DT,
                "company": company,
                "warehouse": warehouse,
                "warehouse_code": warehouse_code,
                "bin_location": bin_location,
                "status": status,
                "external_ref": external_ref,
                "posting_date": posting_date,
            }
        )
        if _meta_has(TASK_DT, "sync_stage"):
            doc.sync_stage = "Captured"
        if _meta_has(TASK_DT, "sync_status"):
            doc.sync_status = "Synced"
        if _meta_has(TASK_DT, "count_mode"):
            doc.count_mode = "Adhoc Add" if count_mode == "adhoc_add" else "Reconciliation"
        if _meta_has(TASK_DT, "include_in_post"):
            doc.include_in_post = 1

        if isinstance(lines, list):
            for i, row in enumerate(lines, start=1):
                if not isinstance(row, dict):
                    continue
                item_code = row.get("item_code") or row.get("item") or row.get("code")
                if not item_code:
                    continue
                row_bin = row.get("bin_location") or bin_location
                counted_qty = _safe_float(row.get("counted_qty") or row.get("qty") or row.get("counted") or 0)
                carton_id_val = _extract_carton_id(row)

                if result_has_carton and (not carton_is_link) and carton_reqd:
                    if not (carton_id_val and str(carton_id_val).strip()):
                        frappe.throw(_("Row {0}: carton_id is required (Item {1}).").format(i, item_code))

                child_dict = {
                    "item_code": item_code,
                    "bin_location": row_bin,
                    "counted_qty": counted_qty,
                    "system_qty": 0.0,
                    "delta_qty": counted_qty,
                    "has_discrepancy": 1 if abs(counted_qty) > 0.000001 else 0,
                }
                if result_has_carton and (not carton_is_link):
                    child_dict["carton_id"] = (carton_id_val or "").strip()

                doc.append("results", child_dict)

                if result_has_carton and (not carton_is_link) and doc.results:
                    doc.results[-1].carton_id = (carton_id_val or "").strip()

        doc.insert(ignore_permissions=True)
        updated_lines = len(doc.results)

    batch_name = link_task_to_batch(doc)

    return {
        "ok": True,
        "api_version": API_VERSION,
        "task": doc.name,
        "external_ref": external_ref,
        "batch": batch_name,
        "count_mode": count_mode,
        "updated_lines": updated_lines,
    }


# ---------------------------------------------------------------------
# API 2: Batch -> Load Actual Stock Preview
# ---------------------------------------------------------------------
@frappe.whitelist()
def load_actual_stock_preview(batch_name: str):
    b = frappe.get_doc(BATCH_DT, batch_name)

    warehouse_code = b.warehouse_code
    if not warehouse_code:
        frappe.throw(_("Batch.warehouse_code is required."))

    task_names = _tasks_for_batch_processing(batch_name)
    if not task_names:
        return {"ok": True, "updated_lines": 0, "summary_rows": 0, "note": "No tasks selected for preview on this batch"}

    result_has_carton = _meta_has(RESULT_DT, "carton_id")

    summary_carton_reqd = False
    if _meta_has(SUMMARY_CHILD_DT, "carton_id"):
        try:
            sf = frappe.get_meta(SUMMARY_CHILD_DT).get_field("carton_id")
            if sf:
                summary_carton_reqd = getattr(sf, "reqd", False)
        except Exception:
            pass

    res_fields = ["name", "parent", "item_code", "bin_location", "counted_qty"]
    if result_has_carton:
        res_fields.append("carton_id")

    res_rows = frappe.get_all(
        RESULT_DT,
        filters={"parent": ["in", task_names]},
        fields=res_fields,
        limit_page_length=200000,
    )

    def _k(r):
        if result_has_carton:
            return (r["item_code"], r["bin_location"], (r.get("carton_id") or ""))
        return (r["item_code"], r["bin_location"])

    # One counted_qty per key -> keep latest row by name (prevents triple totals)
    grouped = {}
    for r in res_rows:
        key = _k(r)
        q = _safe_float(r.get("counted_qty"))
        rname = _row_name(r.get("name"))
        if key not in grouped or _is_later_row_name(rname, grouped[key].get("name")):
            grouped[key] = {"counted": q, "name": rname}

    b.set("summary", [])
    system_map = {}

    for key, agg in grouped.items():
        if result_has_carton:
            item_code, bin_location, carton_id = key
        else:
            item_code, bin_location = key
            carton_id = None

        where = ["warehouse = %s", "location = %s", "item_code = %s"]
        params = [b.warehouse, bin_location, item_code]

        if carton_id and _meta_has(STOCK_BAL_DT, "carton"):
            where.append("carton = %s")
            params.append(carton_id)

        system_qty = frappe.db.sql(
            f"""
            SELECT COALESCE(SUM(qty), 0)
            FROM `tab{STOCK_BAL_DT}`
            WHERE {" AND ".join(where)}
            """,
            tuple(params),
        )[0][0] or 0

        system_qty = float(system_qty)
        counted = float(agg["counted"])
        delta = counted - system_qty

        system_map[key] = system_qty

        summary_row = {
            "item_code": item_code,
            "bin_location": bin_location,
            "total_system_qty": system_qty,
            "total_counted_qty": counted,
            "total_delta_qty": delta,
        }

        if _meta_has(SUMMARY_CHILD_DT, "carton_id"):
            summary_row["carton_id"] = (carton_id or "").strip() if result_has_carton else ""
            if summary_row["carton_id"] == "" and summary_carton_reqd:
                summary_row["carton_id"] = "-"  # placeholder if mandatory

        b.append("summary", summary_row)

    # update each result row system_qty/delta
    updated_lines = 0
    for r in res_rows:
        key = _k(r)
        sys_qty = float(system_map.get(key, 0.0))
        counted = _safe_float(r.get("counted_qty"))
        delta = counted - sys_qty
        has_disc = 1 if abs(delta) > 0.000001 else 0

        frappe.db.set_value(RESULT_DT, r["name"], "system_qty", sys_qty, update_modified=False)
        frappe.db.set_value(RESULT_DT, r["name"], "delta_qty", delta, update_modified=False)
        frappe.db.set_value(RESULT_DT, r["name"], "has_discrepancy", has_disc, update_modified=False)
        updated_lines += 1

    if _meta_has(BATCH_DT, "preview_loaded_on"):
        b.preview_loaded_on = now_datetime()

    if _meta_has(BATCH_DT, "status"):
        b.status = "Previewed"

    for tname in task_names:
        if _meta_has(TASK_DT, "sync_stage"):
            frappe.db.set_value(TASK_DT, tname, "sync_stage", "Previewed", update_modified=False)

    b.save(ignore_permissions=True)

    return {"ok": True, "batch": batch_name, "updated_lines": updated_lines, "summary_rows": len(b.summary)}


# ---------------------------------------------------------------------
# API 3: Export Cycle Count Verification Excel (item-level)
# ---------------------------------------------------------------------
@frappe.whitelist()
def export_opening_valuation_template(batch_name=None):
    """
    Export item-level verification Excel for finance.

    Run load_actual_stock_preview first so previous_qty (system) is populated.
    Finance may override valuation_rate; upload via upload_opening_valuation_file.
    """
    import io
    import openpyxl
    from openpyxl.styles import Font, Alignment, PatternFill
    from openpyxl.utils import get_column_letter
    from frappe.utils.file_manager import save_file

    if not batch_name:
        batch_name = frappe.local.form_dict.get("batch_name")
    if not batch_name:
        frappe.throw(_("batch_name is required"))

    if not _batch_preview_loaded(batch_name):
        frappe.throw(
            _(
                "Run 'Load Actual Stock Preview' before exporting. "
                "Finance needs previous_qty (system) vs counted_qty."
            )
        )

    b = frappe.get_doc(BATCH_DT, batch_name)
    company = b.get("company")
    warehouse = b.get("warehouse")
    posting_date = b.get("posting_date")

    if not company:
        frappe.throw(_("Batch.company is required"))
    if not warehouse:
        frappe.throw(_("Batch.warehouse is required"))

    grouped = _aggregate_item_totals_from_batch(batch_name)
    if not grouped:
        frappe.throw(_("No item totals to export for this batch"))

    item_codes = sorted(grouped.keys())
    item_info = {
        x["name"]: x
        for x in frappe.get_all(
            "Item",
            filters={"name": ["in", item_codes]},
            fields=["name", "item_name", "stock_uom"],
        )
    }
    currency = _company_currency(company)

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = VERIFICATION_SHEET

    headers = [
        "company",
        "warehouse",
        "posting_date",
        "item_code",
        "item_name",
        "uom",
        "wms_previous_qty",
        "counted_qty",
        "wms_delta_qty",
        "erp_current_qty",
        "erp_delta_qty",
        "valuation_rate",
        "erp_value_impact",
        "currency",
        "override_reason",
        "remarks",
    ]
    ws.append(headers)

    header_font = Font(bold=True, color="FFFFFF")
    header_fill = PatternFill("solid", fgColor="2F5597")
    missing_rate_fill = PatternFill("solid", fgColor="FFF2CC")
    mismatch_fill = PatternFill("solid", fgColor="FCE4D6")

    missing_rate_count = 0
    erp_mismatch_count = 0
    for item_code in item_codes:
        totals = grouped[item_code]
        wms_previous_qty = flt(totals.get("previous_qty"))
        counted_qty = flt(totals.get("counted_qty"))
        wms_delta_qty = flt(totals.get("delta_qty"))
        erp_current_qty = _get_erp_bin_qty(item_code, warehouse)
        erp_delta_qty = counted_qty - erp_current_qty
        default_rate = flt(_default_valuation_rate(item_code, warehouse, wms_previous_qty))
        erp_value_impact = erp_delta_qty * default_rate if default_rate else 0.0
        inf = item_info.get(item_code, {})

        if default_rate <= 0:
            missing_rate_count += 1
        if abs(wms_delta_qty - erp_delta_qty) > 0.000001:
            erp_mismatch_count += 1

        row_idx = ws.max_row + 1
        ws.append(
            [
                company,
                warehouse,
                str(posting_date) if posting_date else "",
                item_code,
                inf.get("item_name") or "",
                inf.get("stock_uom") or "",
                wms_previous_qty,
                counted_qty,
                wms_delta_qty,
                erp_current_qty,
                erp_delta_qty,
                default_rate if flt(default_rate) > 0 else "",
                erp_value_impact if flt(default_rate) > 0 else "",
                currency,
                "",
                f"Batch {batch_name}: do NOT change counted_qty; edit valuation_rate only",
            ]
        )
        row_fill = None
        if default_rate <= 0:
            row_fill = missing_rate_fill
        elif abs(wms_delta_qty - erp_delta_qty) > 0.000001:
            row_fill = mismatch_fill
        if row_fill:
            for col in range(1, len(headers) + 1):
                ws.cell(row=row_idx, column=col).fill = row_fill

    for col, h in enumerate(headers, start=1):
        c = ws.cell(row=1, column=col)
        c.font = header_font
        c.fill = header_fill
        c.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        ws.column_dimensions[get_column_letter(col)].width = max(14, min(30, len(h) + 4))

    ws.freeze_panes = "A2"

    ws2 = wb.create_sheet("README")
    readme = [
        "Cycle Count Verification — item-level (grouped by item for ERP Stock Reconciliation).",
        "1) Run Load Actual Stock Preview on the batch.",
        "2) Finance verifies valuation_rate (counted_qty is locked to device totals).",
        "3) Upload this file using 'Upload Verified Excel' to create Stock Reconciliation.",
        "4) Confirm & Post Batch to update WMS Stock Balance.",
        "",
        "IMPORTANT:",
        "- counted_qty is the sum of all device/location counts for that item (e.g. 10+10=20).",
        "- Do NOT edit counted_qty in Excel — upload is rejected if it differs from the batch.",
        "- To fix wrong qty, correct on mobile and re-run Load Actual Stock Preview.",
        "- Finance may override valuation_rate only.",
        "Qty columns:",
        "- wms_previous_qty / wms_delta_qty = WMS Stock Balance (warehouse operations).",
        "- erp_current_qty / erp_delta_qty = ERP Bin qty (what Stock Reconciliation will adjust).",
        "- erp_value_impact = erp_delta_qty x valuation_rate (actual accounting impact).",
        "",
        "Row colors:",
        "- Yellow: valuation_rate missing — finance must fill before upload.",
        "- Orange: WMS delta differs from ERP delta — review before posting.",
        "",
        f"Difference account (adjustment): {_pick_difference_account(opening_entry=0, company=company)}",
    ]
    for i, line in enumerate(readme, start=1):
        ws2.cell(row=i, column=1, value=line).alignment = Alignment(wrap_text=True)
    ws2.column_dimensions["A"].width = 120

    buff = io.BytesIO()
    wb.save(buff)
    data = buff.getvalue()

    filename = f"Cycle-Count-Verification-{batch_name}.xlsx"
    f = save_file(filename, data, BATCH_DT, batch_name, is_private=1)

    return {
        "ok": True,
        "api_version": API_VERSION,
        "batch": batch_name,
        "file_name": f.file_name,
        "file_url": f.file_url,
        "item_count": len(item_codes),
        "missing_rate_count": missing_rate_count,
        "erp_mismatch_count": erp_mismatch_count,
        "sheet_name": VERIFICATION_SHEET,
    }


@frappe.whitelist()
def export_cycle_count_verification_template(batch_name=None):
    """Alias for export_opening_valuation_template."""
    return export_opening_valuation_template(batch_name)

# ---------------------------------------------------------------------
# API 4: Upload Verified Excel -> Create Stock Reconciliation (adjustment)
# ---------------------------------------------------------------------
@frappe.whitelist()
def upload_opening_valuation_file(file_url=None, file_id=None, batch_name=None, submit=1):
    """
    Read Cycle Count Verification Excel and create Stock Reconciliation (adjustment).

    Supports sheet 'Cycle Count Verification' (new) or 'Opening Valuation Upload' (legacy).
    Uses counted_qty (or counted_qty_total) and finance-overridden valuation_rate.
    """
    import openpyxl
    from frappe.utils import cint, nowdate, flt, cstr

    submit = cint(submit or 1)

    allowed_roles = {"Accounts Manager", "Stock Manager", "System Manager"}
    if not any(r in allowed_roles for r in frappe.get_roles(frappe.session.user)):
        frappe.throw(_("Not allowed. Finance/Stock Manager only."))

    if not file_url and not file_id:
        file_url = frappe.local.form_dict.get("file_url")
        file_id = frappe.local.form_dict.get("file_id")
    if not batch_name:
        batch_name = frappe.local.form_dict.get("batch_name")

    if not file_url and not file_id:
        frappe.throw(_("file_url or file_id is required"))

    batch_defaults = {}
    b = None

    if batch_name:
        b = frappe.get_doc(BATCH_DT, batch_name)
        batch_defaults = {
            "company": b.get("company"),
            "warehouse": b.get("warehouse"),
            "posting_date": b.get("posting_date"),
        }

        if _meta_has(BATCH_DT, "stock_reconciliation"):
            existing_sr = (b.get("stock_reconciliation") or "").strip()
            if existing_sr and frappe.db.exists("Stock Reconciliation", existing_sr):
                frappe.throw(
                    _(
                        "Stock Reconciliation already linked for this batch: {0}. "
                        "Cancel that SR first if you need to create a new one."
                    ).format(existing_sr)
                )

    fdoc = None
    if file_id:
        fdoc = frappe.get_doc("File", file_id)
    else:
        file_url = cstr(file_url).strip()
        if not file_url:
            frappe.throw(_("file_url or file_id is required"))
        file_name = frappe.db.get_value("File", {"file_url": file_url}, "name")
        if not file_name:
            frappe.throw(_("File not found for URL: {0}").format(file_url))
        fdoc = frappe.get_doc("File", file_name)

    if not fdoc:
        frappe.throw(_("Could not resolve file. Provide file_id or file_url."))

    file_path = fdoc.get_full_path()
    wb = openpyxl.load_workbook(file_path, data_only=True)

    sheet_name = VERIFICATION_SHEET if VERIFICATION_SHEET in wb.sheetnames else LEGACY_SHEET
    if sheet_name not in wb.sheetnames:
        frappe.throw(_("Sheet '{0}' or '{1}' not found").format(VERIFICATION_SHEET, LEGACY_SHEET))
    ws = wb[sheet_name]

    header = [(c.value or "").strip() if isinstance(c.value, str) else (c.value or "") for c in ws[1]]

    def idx(col):
        try:
            return header.index(col)
        except ValueError:
            return None

    counted_col = "counted_qty" if idx("counted_qty") is not None else "counted_qty_total"
    required_cols = ["company", "warehouse", "posting_date", "item_code", counted_col, "valuation_rate"]
    for col in required_cols:
        if idx(col) is None:
            frappe.throw(_("Missing column in Excel: {0}").format(col))

    parsed = []
    for r in range(2, ws.max_row + 1):
        vals = [ws.cell(row=r, column=c).value for c in range(1, ws.max_column + 1)]
        if not any(vals):
            continue

        company = vals[idx("company")] or batch_defaults.get("company")
        warehouse = vals[idx("warehouse")] or batch_defaults.get("warehouse")
        posting_date = vals[idx("posting_date")] or batch_defaults.get("posting_date") or nowdate()

        item_code = vals[idx("item_code")]
        if isinstance(item_code, str):
            item_code = item_code.strip()

        counted_qty = vals[idx(counted_col)] or 0
        valuation_rate = vals[idx("valuation_rate")] or 0

        if not company or not warehouse:
            frappe.throw(_("Row {0}: company/warehouse is required (or provide batch_name)").format(r))
        if not item_code:
            frappe.throw(_("Row {0}: item_code is required").format(r))

        try:
            counted_qty = float(counted_qty or 0)
        except Exception:
            frappe.throw(_("Row {0}: {1} must be a number").format(r, counted_col))

        try:
            valuation_rate = float(valuation_rate or 0)
        except Exception:
            frappe.throw(_("Row {0}: valuation_rate must be a number").format(r))

        if counted_qty < 0:
            frappe.throw(_("Row {0}: counted qty cannot be negative").format(r))
        if valuation_rate <= 0:
            frappe.throw(_("Row {0}: valuation_rate is required (> 0) for item {1}").format(r, item_code))

        if not frappe.db.exists("Item", item_code):
            frappe.throw(_("Row {0}: Item not found: {1}").format(r, item_code))

        parsed.append(
            {
                "company": str(company).strip(),
                "warehouse": str(warehouse).strip(),
                "posting_date": posting_date,
                "item_code": str(item_code).strip(),
                "qty": counted_qty,
                "valuation_rate": valuation_rate,
            }
        )

    if not parsed:
        frappe.throw(_("No valid rows found in Excel"))

    first_company = parsed[0]["company"]
    first_wh = parsed[0]["warehouse"]
    first_posting_date = parsed[0]["posting_date"] or nowdate()
    posting_time = nowtime()

    for x in parsed:
        if x["company"] != first_company or x["warehouse"] != first_wh:
            frappe.throw(_("Excel must contain one company + one warehouse only (for safety)."))

    agg = {}
    for x in parsed:
        item = x["item_code"]
        q = float(x["qty"] or 0)
        v = float(x["valuation_rate"] or 0)
        if item not in agg:
            agg[item] = {"qty": 0.0, "total_val": 0.0}
        agg[item]["qty"] += q
        agg[item]["total_val"] += q * v

    if batch_name:
        uploaded_totals = {item: flt(a["qty"]) for item, a in agg.items()}
        _validate_upload_counts_match_batch(batch_name, uploaded_totals)

    rows = []
    for item, a in agg.items():
        qty = float(a["qty"] or 0)
        rate = (float(a["total_val"]) / qty) if qty else 0.0
        rows.append({"item_code": item, "qty": qty, "valuation_rate": rate})

    diff_acc = _pick_difference_account(opening_entry=0, company=first_company)
    _ensure_account_exists(diff_acc)

    SR_DT = "Stock Reconciliation"
    SR_ITEM_DT = "Stock Reconciliation Item"
    sr_meta = frappe.get_meta(SR_DT)
    sri_meta = frappe.get_meta(SR_ITEM_DT)

    qty_field = "qty" if sri_meta.has_field("qty") else ("quantity" if sri_meta.has_field("quantity") else None)
    if not qty_field:
        frappe.throw(_("SR Item has no qty/quantity field."))

    val_field = "valuation_rate" if sri_meta.has_field("valuation_rate") else ("rate" if sri_meta.has_field("rate") else None)
    if not val_field:
        frappe.throw(_("SR Item has no valuation_rate field."))

    filtered = []
    no_change = []
    for x in sorted(rows, key=lambda z: z["item_code"]):
        cur_qty, cur_rate = _get_stock_snapshot(x["item_code"], first_wh, str(first_posting_date), posting_time)
        new_qty = flt(x["qty"])
        new_rate = flt(x["valuation_rate"])
        if abs(new_qty - cur_qty) < 1e-9 and abs(new_rate - cur_rate) < 1e-9:
            no_change.append(
                {
                    "item_code": x["item_code"],
                    "current_qty": cur_qty,
                    "new_qty": new_qty,
                    "current_rate": cur_rate,
                    "new_rate": new_rate,
                }
            )
        else:
            filtered.append(x)

    if not filtered:
        return {
            "ok": False,
            "api_version": API_VERSION,
            "message": "No change detected for all items. Stock Reconciliation not created.",
            "company": first_company,
            "warehouse": first_wh,
            "posting_date": first_posting_date,
            "difference_account": diff_acc,
            "no_change_count": len(no_change),
            "sample_no_change": no_change[:10],
        }

    rows = filtered

    sr = frappe.get_doc({"doctype": SR_DT})

    if sr_meta.has_field("company"):
        sr.company = first_company
    if sr_meta.has_field("posting_date"):
        sr.posting_date = first_posting_date
    if sr_meta.has_field("posting_time"):
        sr.posting_time = posting_time
    if sr_meta.has_field("purpose"):
        df = sr_meta.get_field("purpose")
        opts = [x.strip() for x in (df.options or "").split("\n") if x.strip()]
        preferred = "Stock Reconciliation"
        sr.purpose = preferred if preferred in opts else (opts[0] if opts else preferred)
    if sr_meta.has_field("opening_entry"):
        sr.opening_entry = 0
    if sr_meta.has_field("set_warehouse"):
        sr.set_warehouse = first_wh
    if sr_meta.has_field("expense_account"):
        sr.expense_account = diff_acc
    if sr_meta.has_field("difference_account"):
        sr.difference_account = diff_acc

    sr.set("items", [])

    seen = set()
    for x in sorted(rows, key=lambda z: z["item_code"]):
        key = (x["item_code"], first_wh)
        if key in seen:
            frappe.throw(_("Duplicate item+warehouse detected: Item {0} Warehouse {1}").format(x["item_code"], first_wh))
        seen.add(key)

        sr.append(
            "items",
            {
                "item_code": x["item_code"],
                "warehouse": first_wh,
                qty_field: float(x["qty"]),
                val_field: float(x["valuation_rate"]),
            },
        )

    sr.insert(ignore_permissions=True)

    if submit:
        sr.submit()

    if batch_name and _meta_has(BATCH_DT, "stock_reconciliation"):
        b = frappe.get_doc(BATCH_DT, batch_name)
        b.stock_reconciliation = sr.name
        b.save(ignore_permissions=True)

    return {
        "ok": True,
        "api_version": API_VERSION,
        "mode": "adjustment",
        "sr": sr.name,
        "docstatus": sr.docstatus,
        "company": first_company,
        "warehouse": first_wh,
        "difference_account": diff_acc,
        "row_count_excel": len(parsed),
        "row_count_after_agg": len(agg),
        "row_count_in_sr": len(rows),
        "skipped_no_change_count": len(no_change),
        "skipped_sample": no_change[:10],
        "sheet_name": sheet_name,
    }


@frappe.whitelist()
def upload_cycle_count_verification_file(file_url=None, file_id=None, batch_name=None, submit=1):
    """Alias for upload_opening_valuation_file."""
    return upload_opening_valuation_file(file_url=file_url, file_id=file_id, batch_name=batch_name, submit=submit)

# ---------------------------------------------------------------------
# API 5: Confirm & Post Batch
# ---------------------------------------------------------------------
@frappe.whitelist()
def confirm_and_post_batch(batch_name=None, create_stock_reconciliation=0, is_opening=0):
    """
    Update WMS Stock Balance and link Stock Reconciliation from upload (API 4).

    Recommended flow:
      1) load_actual_stock_preview
      2) export_opening_valuation_template
      3) upload_opening_valuation_file (creates SR)
      4) confirm_and_post_batch (WMS balances; reuses linked SR)

    create_stock_reconciliation=1 is a legacy fallback if SR was not uploaded first.
    """
    create_stock_reconciliation = cint(create_stock_reconciliation or 0)
    is_opening = cint(is_opening or 0)

    if not batch_name:
        batch_name = frappe.local.form_dict.get("batch_name")
    if not batch_name:
        frappe.throw(_("batch_name is required"))

    b = frappe.get_doc(BATCH_DT, batch_name)
    status = (b.get("status") or "").strip()

    # ------------------------------------------------------------------
    # 0) If already posted -> stop here (prevents duplicate SR on re-click)
    # ------------------------------------------------------------------
    if status == "Posted":
        existing_sr = (b.get("stock_reconciliation") or "").strip() if _meta_has(BATCH_DT, "stock_reconciliation") else ""
        existing_sr = existing_sr or None
        return {
            "ok": True,
            "api_version": API_VERSION,
            "batch": batch_name,
            "mode": "opening" if cint(is_opening) else "adjustment",
            "message": "Batch already posted. No action taken.",
            "updated_balances": 0,
            "sr": existing_sr,
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

    # Opening SR must exist (created by API 4)
    opening_sr_field = "opening_stock_reconciliation"
    opening_sr_name = b.get(opening_sr_field) if _meta_has(BATCH_DT, opening_sr_field) else None
    if is_opening and not opening_sr_name:
        frappe.throw(_("Opening mode: Upload valuation Excel and create Opening Stock SR first (API 4)."))

    nowdt = now_datetime()

    # ------------------------------------------------------------------
    # 1) Load tasks + results
    # ------------------------------------------------------------------
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
        for t in frappe.get_all(
            TASK_DT, filters={"name": ["in", task_names]}, fields=["name", "count_mode"]
        ):
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

    # ------------------------------------------------------------------
    # 2) Update WMS Stock Balance by (item, location, carton)
    # ------------------------------------------------------------------
    wms_group = _build_wms_count_map(res_rows, has_carton_id)
    cleared_stale_cartons, cleared_cartons = _zero_stale_wms_cartons(
        company,
        warehouse,
        wms_group,
        nowdt,
        batch_name=batch_name,
        reconciliation_items=reconciliation_items,
        adhoc_location_items=adhoc_location_items,
    )

    updated_balances = 0
    for (item_code, location, carton_key), counted_qty in wms_group.items():
        carton = carton_key or None

        if not frappe.db.exists("Item", item_code):
            frappe.throw(_("Item not found: {0}").format(item_code))

        filters = {"company": company, "warehouse": warehouse, "item_code": item_code, "location": location}
        if carton:
            filters["carton"] = carton
        else:
            filters["carton"] = ["in", ["", None]]

        existing = frappe.db.get_value(STOCK_BAL_DT, filters, ["name", "qty"], as_dict=True)
        previous_qty = flt(existing.get("qty")) if existing else 0.0

        if existing:
            frappe.db.set_value(STOCK_BAL_DT, existing.name, "qty", float(counted_qty), update_modified=False)
            if _meta_has(STOCK_BAL_DT, "last_txn_datetime"):
                frappe.db.set_value(STOCK_BAL_DT, existing.name, "last_txn_datetime", nowdt, update_modified=False)
        else:
            bal = frappe.get_doc(
                {
                    "doctype": STOCK_BAL_DT,
                    "company": company,
                    "warehouse": warehouse,
                    "item_code": item_code,
                    "location": location,
                    "carton": carton,
                    "qty": float(counted_qty),
                }
            )
            if _meta_has(STOCK_BAL_DT, "last_txn_datetime"):
                bal.last_txn_datetime = nowdt
            bal.insert(ignore_permissions=True)

        _write_cycle_count_ledger_set(
            company=company,
            warehouse=warehouse,
            batch_name=batch_name,
            item_code=item_code,
            location=location,
            carton=(carton or "").strip(),
            previous_qty=previous_qty,
            new_qty=float(counted_qty),
            nowdt=nowdt,
        )

        updated_balances += 1

    # ------------------------------------------------------------------
    # 3) Stock Reconciliation (NO DUPLICATES + NO "NO CHANGE" POPUP)
    # ------------------------------------------------------------------
    sr_name = None
    sr_note = None

    existing_batch_sr = None
    if _meta_has(BATCH_DT, "stock_reconciliation"):
        existing_batch_sr = (b.get("stock_reconciliation") or "").strip() or None
        if existing_batch_sr and not frappe.db.exists("Stock Reconciliation", existing_batch_sr):
            existing_batch_sr = None

    if is_opening:
        # Opening: SR comes from API 4 only
        sr_name = opening_sr_name
        sr_note = "Opening mode: SR not created here; linked API4 Opening SR."

        # optional: store it for easy reference in UI (do not override)
        if _meta_has(BATCH_DT, "stock_reconciliation") and not existing_batch_sr and sr_name:
            b.stock_reconciliation = sr_name

    else:
        # Adjustment:
        if existing_batch_sr:
            sr_name = existing_batch_sr
            sr_note = "Adjustment SR already linked on batch; reused (no duplicate)."
        else:
            if create_stock_reconciliation:
                # group by item_code
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

                val_field = "valuation_rate" if sri_meta.has_field("valuation_rate") else ("rate" if sri_meta.has_field("rate") else None)

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

                # IMPORTANT: catch ERPNext "no change" error and SKIP SR
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
                        # Do not throw -> no popup, batch can still be posted
                        sr_name = None
                        sr_note = "Adjustment mode: ERPNext detected no change; SR skipped."
                    else:
                        raise

            else:
                sr_note = "Adjustment mode: create_stock_reconciliation=0; SR not created."

    # ------------------------------------------------------------------
    # 4) Post the batch + tasks
    # ------------------------------------------------------------------
    for tname in task_names:
        if _meta_has(TASK_DT, "status"):
            frappe.db.set_value(TASK_DT, tname, "status", "Posted", update_modified=False)
        if _meta_has(TASK_DT, "sync_stage"):
            frappe.db.set_value(TASK_DT, tname, "sync_stage", "Posted", update_modified=False)

    if _meta_has(BATCH_DT, "status"):
        pending = frappe.db.count(TASK_DT, {"batch": batch_name, "status": ["!=", "Posted"]})
        b.status = "Previewed" if pending else "Posted"
    if _meta_has(BATCH_DT, "posted_on"):
        b.posted_on = nowdt
    if _meta_has(BATCH_DT, "posted_by"):
        b.posted_by = frappe.session.user

    b.save(ignore_permissions=True)

    return {
        "ok": True,
        "api_version": API_VERSION,
        "batch": batch_name,
        "mode": "opening" if is_opening else "adjustment",
        "updated_balances": updated_balances,
        "cleared_stale_cartons": cleared_stale_cartons,
        "cleared_cartons": cleared_cartons,
        "sr": sr_name,
        "sr_note": sr_note,
        "difference_account_opening": _pick_difference_account(opening_entry=1, company=company),
        "difference_account_adjustment": _pick_difference_account(opening_entry=0, company=company),
    }


# ---------------------------------------------------------------------
# API 6: Desktop stock pull / batch sync
# ---------------------------------------------------------------------
@frappe.whitelist()
def get_stock_balance_compact(
    company=None,
    warehouse=None,
    item_code=None,
    last_txn_after=None,
    include_zero=0,
    limit=500,
    offset=0,
):
    """
    Pull WMS Stock Balance for desktop incremental sync.

    Desktop should call this after cycle count post (or periodically) using
    last_txn_after cursor. Rows with qty=0 are included when last_txn_after
    is set so cleared cartons can be removed locally.
    """
    company = (company or frappe.defaults.get_user_default("Company") or "").strip()
    if not company:
        frappe.throw(_("company is required"))

    limit = cint(limit) or 500
    offset = cint(offset) or 0
    include_zero = cint(include_zero or 0)

    filters = {"company": company}
    if warehouse:
        filters["warehouse"] = warehouse
    if item_code:
        filters["item_code"] = item_code
    if last_txn_after:
        filters["last_txn_datetime"] = (">", last_txn_after)
    elif not include_zero:
        filters["qty"] = [">", 0]

    fields = [
        "name",
        "company",
        "warehouse",
        "item_code",
        "location",
        "carton",
        "qty",
        "reserved_qty",
        "last_txn_datetime",
        "modified",
    ]

    rows = frappe.get_all(
        STOCK_BAL_DT,
        filters=filters,
        fields=fields,
        order_by="last_txn_datetime asc, modified asc",
        limit_start=offset,
        limit_page_length=limit + 1,
    )

    has_more = len(rows) > cint(limit)
    if has_more:
        rows = rows[:limit]

    next_cursor = None
    if rows:
        last = rows[-1]
        next_cursor = last.get("last_txn_datetime") or last.get("modified")

    return {
        "ok": True,
        "rows": rows,
        "count": len(rows),
        "has_more": has_more,
        "next_last_txn_after": next_cursor,
    }


@frappe.whitelist()
def get_cycle_count_stock_sync(batch_name: str):
    """
    Return server-side WMS stock truth for all items in a cycle count batch.
    Desktop calls this after ERP posts the batch to reconcile local cache.
    """
    if not batch_name:
        frappe.throw(_("batch_name is required"))

    b = frappe.get_doc(BATCH_DT, batch_name)
    company = (b.get("company") or "").strip()
    warehouse = (b.get("warehouse") or "").strip()
    if not company or not warehouse:
        frappe.throw(_("Batch company/warehouse is required"))

    item_codes = set()
    if b.get("summary"):
        for row in b.summary:
            code = cstr(row.get("item_code")).strip()
            if code:
                item_codes.add(code)

    if not item_codes:
        task_names = frappe.get_all(TASK_DT, filters={"batch": batch_name}, pluck="name")
        if task_names:
            for row in frappe.get_all(
                RESULT_DT,
                filters={"parent": ["in", task_names]},
                fields=["item_code"],
                limit_page_length=200000,
            ):
                code = cstr(row.get("item_code")).strip()
                if code:
                    item_codes.add(code)

    if not item_codes:
        batch_status = (b.get("status") or "").strip()
        ready = batch_status == "Posted"
        return {
            "ok": True,
            "ready": ready,
            "message": _("No items found for this batch."),
            "batch": batch_name,
            "status": batch_status,
            "items": [],
            "balances": [],
            "cleared_cartons": [],
        }

    balances = frappe.get_all(
        STOCK_BAL_DT,
        filters={
            "company": company,
            "warehouse": warehouse,
            "item_code": ["in", list(item_codes)],
        },
        fields=[
            "name",
            "item_code",
            "location",
            "carton",
            "qty",
            "reserved_qty",
            "last_txn_datetime",
        ],
        order_by="item_code asc, location asc, carton asc",
        limit_page_length=200000,
    )

    cleared_cartons = []
    ledger_rows = frappe.get_all(
        "WMS Stock Ledger Entry",
        filters={
            "voucher_doctype": BATCH_DT,
            "voucher_name": batch_name,
            "event_type": "CycleCount",
            "qty_after": 0,
        },
        fields=["item_code", "location", "carton", "qty_change", "posting_datetime"],
        limit_page_length=200000,
    )
    for row in ledger_rows:
        cleared_cartons.append(
            {
                "item_code": row.get("item_code"),
                "location": row.get("location"),
                "carton": (row.get("carton") or "").strip(),
                "previous_qty": abs(flt(row.get("qty_change"))),
                "qty_after": 0,
                "posting_datetime": row.get("posting_datetime"),
            }
        )

    items_summary = {}
    for bal in balances:
        code = bal.get("item_code")
        items_summary.setdefault(code, 0.0)
        items_summary[code] += flt(bal.get("qty"))

    batch_status = (b.get("status") or "").strip()
    ready = batch_status == "Posted"

    return {
        "ok": True,
        "ready": ready,
        "message": (
            _("Stock sync ready. Apply balances and cleared cartons locally.")
            if ready
            else _(
                "Batch is not Posted on ERP yet. Complete Preview → Upload SR → Confirm & Post, then sync again."
            )
        ),
        "batch": batch_name,
        "status": batch_status,
        "company": company,
        "warehouse": warehouse,
        "items": [
            {"item_code": code, "wms_total_qty": qty}
            for code, qty in sorted(items_summary.items())
        ],
        "balances": balances,
        "cleared_cartons": cleared_cartons,
    }


@frappe.whitelist()
def get_item_wms_stock_for_desktop(
    item_code=None,
    warehouse=None,
    company=None,
    include_zero=0,
):
    """Server truth for desktop Item list + Location Breakdown after cycle count post."""
    item_code = cstr(item_code).strip()
    if not item_code:
        frappe.throw(_("item_code is required"))

    company = (company or frappe.defaults.get_user_default("Company") or "").strip()
    warehouse = cstr(warehouse).strip()
    include_zero = cint(include_zero or 0)

    filters = {"item_code": item_code}
    if company:
        filters["company"] = company
    if warehouse:
        filters["warehouse"] = warehouse
    if not include_zero:
        filters["qty"] = [">", 0]

    rows = frappe.get_all(
        STOCK_BAL_DT,
        filters=filters,
        fields=[
            "name",
            "company",
            "warehouse",
            "item_code",
            "location",
            "carton",
            "qty",
            "reserved_qty",
            "last_txn_datetime",
            "modified",
        ],
        order_by="location asc, carton asc",
        limit_page_length=2000,
    )

    total_qty = 0.0
    breakdown = []
    for r in rows:
        qty = flt(r.get("qty"))
        reserved = flt(r.get("reserved_qty"))
        total_qty += qty
        breakdown.append(
            {
                "location_id": r.get("location"),
                "carton_id": (r.get("carton") or "").strip(),
                "warehouse": r.get("warehouse"),
                "in_qty": 0,
                "out_qty": 0,
                "balance_qty": qty,
                "reserved_qty": reserved,
                "available_qty": qty - reserved,
                "last_txn_datetime": r.get("last_txn_datetime"),
            }
        )

    return {
        "ok": True,
        "item_code": item_code,
        "company": company,
        "warehouse": warehouse or None,
        "total_balance_qty": total_qty,
        "rows": breakdown,
        "count": len(breakdown),
    }