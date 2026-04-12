# -*- coding: utf-8 -*-
from __future__ import annotations

import frappe
from frappe import _
from frappe.utils import now_datetime, getdate

# ============================================================
# CONFIG
# ============================================================

API_VERSION = "asn_pr_only_v4_final_plus_warehouse_v2_no_dupes"

ASN_DOCTYPE = "WMS ASN"
ASN_ITEM_DOCTYPE = "WMS ASN Item"

# Rate rounding precision used for grouping (prevents float/precision duplicates)
RATE_GROUP_PRECISION = 6

# If True: group by item_code ONLY (single line per item, uses weighted avg rate)
# If False: group by item_code + rounded rate
GROUP_BY_ITEM_ONLY = False


# ============================================================
# HELPERS
# ============================================================

def _get_payload() -> dict:
    data = dict(frappe.form_dict or {})
    try:
        j = frappe.request.get_json(silent=True)
        if isinstance(j, dict):
            data.update(j)
    except Exception:
        pass
    return data


def _get_meta_safe(doctype: str):
    try:
        return frappe.get_meta(doctype, ignore_permissions=True)
    except TypeError:
        return frappe.get_meta(doctype)
    except Exception:
        return frappe.get_meta(doctype)


def _has_field(doctype: str, fieldname: str) -> bool:
    try:
        return _get_meta_safe(doctype).has_field(fieldname)
    except Exception:
        return False


def _set_if_exists(doc, fieldname: str, value):
    """Set a field only if it exists in that DocType."""
    if value is None:
        return
    if _has_field(doc.doctype, fieldname):
        doc.set(fieldname, value)


def _as_flt(v, default=0.0) -> float:
    try:
        return float(v or 0)
    except Exception:
        return default


def _as_int(v, default=0) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _detect_received_field() -> str:
    """Auto detect received qty field"""
    meta = _get_meta_safe(ASN_ITEM_DOCTYPE)
    for f in ["received_qty", "recvd_qty", "qty_received", "received"]:
        if meta.has_field(f):
            return f
    frappe.throw(_("No received quantity field found in WMS ASN Item"))


def _detect_correct_parentfield(asn_no: str, received_field: str) -> str:
    """
    Detect which ASN table actually contains inbound items.
    Prevents duplicate reads when ASN has multiple tables.
    """
    rows = frappe.db.sql(
        f"""
        SELECT
            parentfield,
            COUNT(*) AS row_count,
            SUM(COALESCE(`{received_field}`,0)) AS total_received
        FROM `tab{ASN_ITEM_DOCTYPE}`
        WHERE parent=%s AND parenttype=%s
        GROUP BY parentfield
        """,
        (asn_no, ASN_DOCTYPE),
        as_dict=True,
    )

    if not rows:
        frappe.throw(_("ASN has no item rows"))

    rows.sort(
        key=lambda x: (
            float(x.get("total_received") or 0),
            int(x.get("row_count") or 0),
        ),
        reverse=True,
    )
    return rows[0]["parentfield"]


def _group_items(rows: list[dict], received_field: str, group_items: int, rate_precision: int = RATE_GROUP_PRECISION):
    """
    Merge duplicate item rows into PR lines.

    Fixes duplicate PR items caused by:
      - slight unit_cost float differences
      - grouping by (item_code, rate) with high precision
      - optional single-line-per-item mode with weighted avg rate
    """
    grouped: dict = {}

    for r in rows:
        qty = _as_flt(r.get(received_field))
        if qty <= 0:
            continue

        item_code = r.get("item_code")
        if not item_code:
            continue

        rate_raw = _as_flt(r.get("unit_cost"))
        rate_key = round(rate_raw, rate_precision)

        if group_items:
            if GROUP_BY_ITEM_ONLY:
                key = (item_code,)
            else:
                key = (item_code, rate_key)
        else:
            # keep rows separate (but still normalize rate so PR line rate is clean)
            key = (item_code, rate_key, r.get("idx"))

        if key not in grouped:
            grouped[key] = {
                "item_code": item_code,
                "qty": 0.0,
                "rate": rate_key,        # final rate for PR line (may be adjusted below)
                "_amt": 0.0,             # for weighted avg
            }

        grouped[key]["qty"] += qty
        grouped[key]["_amt"] += qty * rate_raw

    # If we group by item_code only, compute weighted average rate per item
    if group_items and GROUP_BY_ITEM_ONLY:
        for k, g in grouped.items():
            qty = _as_flt(g.get("qty"))
            if qty > 0:
                avg = g["_amt"] / qty
                g["rate"] = round(avg, rate_precision)

    # drop internal
    for g in grouped.values():
        g.pop("_amt", None)

    return grouped


def _acquire_asn_lock(asn_no: str):
    """
    Prevent double PR creation when two devices call API simultaneously.
    Uses MySQL GET_LOCK. Works only if DB is MariaDB/MySQL.
    """
    lock_name = f"asn_pr_lock::{asn_no}"
    try:
        # wait up to 10 seconds
        got = frappe.db.sql("SELECT GET_LOCK(%s, %s) AS got", (lock_name, 10), as_dict=True)
        if got and int(got[0].get("got") or 0) == 1:
            return lock_name
    except Exception:
        # If DB doesn't support locks, just skip
        return None

    frappe.throw(_("Could not acquire lock for ASN {0}. Please retry.").format(asn_no))


def _release_asn_lock(lock_name: str | None):
    if not lock_name:
        return
    try:
        frappe.db.sql("SELECT RELEASE_LOCK(%s)", (lock_name,))
    except Exception:
        pass


# ============================================================
# STATUS CHECK API
# ============================================================

@frappe.whitelist()
def get_pr_status_for_asn(asn_no=None):
    asn_no = asn_no or frappe.form_dict.get("asn_no")
    if not asn_no:
        frappe.throw(_("asn_no is required"))

    if not frappe.db.exists(ASN_DOCTYPE, asn_no):
        return {"ok": False, "reason": "ASN not found"}

    pr_no = frappe.db.get_value(ASN_DOCTYPE, asn_no, "purchase_receipt")

    if not pr_no:
        return {
            "ok": True,
            "purchase_receipt_created": False,
            "purchase_receipt_submitted": False,
        }

    if not frappe.db.exists("Purchase Receipt", pr_no):
        return {"ok": True, "warning": "PR linked but missing"}

    docstatus = frappe.db.get_value("Purchase Receipt", pr_no, "docstatus")

    return {
        "ok": True,
        "purchase_receipt": pr_no,
        "docstatus": docstatus,
        "submitted": docstatus == 1,
    }


# ============================================================
# MAIN API
# ============================================================

@frappe.whitelist(methods=["POST"])
def receive_asn_and_create_purchase_receipt(
    asn_no=None,
    warehouse=None,
    make_pr=1,
    group_items=1,
    rate_precision=None,
):
    """
    Creates Purchase Receipt from WMS ASN received quantities.

    Key improvements:
      1) Prevent duplicate PR lines caused by float unit_cost differences:
         - rate grouping uses rounded rate_key
      2) Optional: group by item_code only (weighted avg rate) using GROUP_BY_ITEM_ONLY=True
      3) Prevent duplicate PR creation via MySQL GET_LOCK (race condition safe)
      4) Sets both PR header set_warehouse (if exists) and PR item warehouse
    """
    payload = _get_payload()

    asn_no = asn_no or payload.get("asn_no")
    warehouse = warehouse or payload.get("warehouse")
    make_pr = _as_int(payload.get("make_pr", make_pr) or 1, 1)
    group_items = _as_int(payload.get("group_items", group_items) or 1, 1)

    # allow per-call override
    rate_precision = _as_int(payload.get("rate_precision", rate_precision or RATE_GROUP_PRECISION), RATE_GROUP_PRECISION)

    if not asn_no:
        frappe.throw(_("asn_no is required"))

    if not warehouse:
        frappe.throw(_("warehouse is required"))

    if not frappe.db.exists(ASN_DOCTYPE, asn_no):
        frappe.throw(_("ASN not found"))

    supplier = frappe.db.get_value(ASN_DOCTYPE, asn_no, "supplier")
    if not supplier:
        frappe.throw(_("Supplier missing in ASN"))

    if not make_pr:
        return {"ok": True, "make_pr": 0, "version": API_VERSION}

    lock_name = None
    try:
        # --------------------------------------------------------
        # LOCK (prevents race condition duplicates)
        # --------------------------------------------------------
        lock_name = _acquire_asn_lock(asn_no)

        # --------------------------------------------------------
        # IDEMPOTENCY CHECK (inside lock)
        # --------------------------------------------------------
        existing_pr = frappe.db.get_value(ASN_DOCTYPE, asn_no, "purchase_receipt")
        if existing_pr and frappe.db.exists("Purchase Receipt", existing_pr):
            return {"ok": True, "already_exists": 1, "purchase_receipt": existing_pr, "version": API_VERSION}

        # --------------------------------------------------------
        # DETECT FIELDS
        # --------------------------------------------------------
        received_field = _detect_received_field()
        parentfield = _detect_correct_parentfield(asn_no, received_field)

        fields = ["item_code", received_field, "idx", "parentfield"]
        if _has_field(ASN_ITEM_DOCTYPE, "unit_cost"):
            fields.append("unit_cost")

        # --------------------------------------------------------
        # LOAD ASN ITEMS (FILTERED)
        # --------------------------------------------------------
        rows = frappe.get_all(
            ASN_ITEM_DOCTYPE,
            filters={
                "parent": asn_no,
                "parenttype": ASN_DOCTYPE,
                "parentfield": parentfield,
            },
            fields=fields,
            order_by="idx asc",
        )

        grouped = _group_items(rows, received_field, group_items, rate_precision=rate_precision)
        if not grouped:
            frappe.throw(_("No received quantity found"))

        # --------------------------------------------------------
        # CREATE PURCHASE RECEIPT
        # --------------------------------------------------------
        pr = frappe.new_doc("Purchase Receipt")
        pr.flags.ignore_permissions = True

        pr.supplier = supplier
        pr.posting_date = getdate(now_datetime())

        # header set_warehouse (if field exists)
        _set_if_exists(pr, "set_warehouse", warehouse)

        for g in grouped.values():
            pr.append(
                "items",
                {
                    "item_code": g["item_code"],
                    "qty": g["qty"],
                    "warehouse": warehouse,   # PR item warehouse
                    "rate": g["rate"],
                }
            )

        # Optional defaults/validation
        try:
            pr.set_missing_values()
        except Exception:
            pass

        # insert draft
        pr.insert()

        # link back to ASN
        frappe.db.set_value(
            ASN_DOCTYPE,
            asn_no,
            "purchase_receipt",
            pr.name,
            update_modified=True,
        )

        return {
            "ok": True,
            "version": API_VERSION,
            "asn_no": asn_no,
            "purchase_receipt": pr.name,
            "supplier": supplier,
            "warehouse": warehouse,
            "set_warehouse_set": bool(_has_field("Purchase Receipt", "set_warehouse")),
            "picked_parentfield": parentfield,
            "source_rows": len(rows),
            "pr_lines": len(pr.items),
            "grouped": bool(group_items),
            "group_mode": ("item_only_weighted_avg" if (group_items and GROUP_BY_ITEM_ONLY) else "item_and_rate"),
            "rate_precision": rate_precision,
            "docstatus": pr.docstatus,
        }

    finally:
        _release_asn_lock(lock_name)