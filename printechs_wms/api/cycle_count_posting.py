# -*- coding: utf-8 -*-
from __future__ import annotations

import frappe
from frappe.utils import now_datetime, getdate


@frappe.whitelist()
def post_cycle_count_batch_grouped():
    """
    Create a Stock Reconciliation in ERPNext using item-wise grouped counted qty.
    Payload (JSON in request body):
      company, warehouse, posting_date, posting_time, batch_id, purpose, items[{item_code, qty}]
    """
    data = frappe.local.form_dict
    if "data" in data and isinstance(data["data"], str):
        # if sent as form field "data" JSON string
        data = frappe.parse_json(data["data"])
    else:
        data = frappe.parse_json(frappe.request.data or "{}") or data

    company = (data.get("company") or "").strip()
    warehouse = (data.get("warehouse") or "").strip()
    posting_date = data.get("posting_date")
    posting_time = data.get("posting_time")
    batch_id = (data.get("batch_id") or "").strip()
    purpose = (data.get("purpose") or "Stock Reconciliation").strip()
    items = data.get("items") or []

    if not company:
        frappe.throw("company is required")
    if not warehouse:
        frappe.throw("warehouse is required")
    if not posting_date:
        posting_date = str(getdate(now_datetime()))
    if not posting_time:
        posting_time = now_datetime().strftime("%H:%M:%S")
    if not batch_id:
        frappe.throw("batch_id is required")
    if not items:
        frappe.throw("items is required")

    # --- consolidate again on server (safety) ---
    grouped = {}
    for row in items:
        item_code = (row.get("item_code") or "").strip()
        qty = float(row.get("qty") or 0)
        if not item_code:
            continue
        grouped[item_code] = grouped.get(item_code, 0.0) + qty

    if not grouped:
        frappe.throw("No valid item rows to post")

    # Optional: prevent duplicate posting for same batch
    existing = frappe.db.get_value(
        "Stock Reconciliation",
        {"remarks": ["like", f"%WMS_BATCH:{batch_id}%"], "docstatus": ["!=", 2]},
        "name",
    )
    if existing:
        return {"ok": True, "already_posted": True, "stock_reconciliation": existing, "batch_id": batch_id}

    sr = frappe.new_doc("Stock Reconciliation")
    sr.company = company
    sr.purpose = "Stock Reconciliation"
    sr.posting_date = posting_date
    sr.posting_time = posting_time
    sr.set_posting_time = 1
    sr.remarks = f"WMS Cycle Count Batch Posting\nWMS_BATCH:{batch_id}\nMODE:{purpose}"

    # Add rows (ERPNext may use child table name: "items")
    for item_code, qty in grouped.items():
        sr.append("items", {
            "item_code": item_code,
            "warehouse": warehouse,
            # ERPNext field name is usually "qty"
            "qty": qty,
        })

    sr.insert(ignore_permissions=True)
    # submit? (you can keep draft if you want manual approval)
    sr.submit()

    return {
        "ok": True,
        "batch_id": batch_id,
        "stock_reconciliation": sr.name,
        "docstatus": sr.docstatus,
        "posted": True,
        "item_count": len(grouped),
    }
