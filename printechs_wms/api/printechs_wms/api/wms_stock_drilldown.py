# -*- coding: utf-8 -*-
"""WMS Stock Drilldown: get_item_location_carton_balance."""
from __future__ import annotations

import frappe
from frappe.utils import cstr, flt


@frappe.whitelist()
def get_item_location_carton_balance(item_code=None, warehouse=None, company=None, limit=500):
    """
    Return WMS Stock Balance rows for an item (optionally by warehouse/company).
    """
    if not item_code or not cstr(item_code).strip():
        return {"ok": False, "rows": [], "count": 0, "message": "item_code is required"}

    item_code = cstr(item_code).strip()
    warehouse = cstr(warehouse).strip() if warehouse else None
    company = cstr(company).strip() if company else None
    limit = int(limit or 500)

    DT = "WMS Stock Balance"
    if not frappe.db.exists("DocType", DT):
        return {"ok": False, "rows": [], "count": 0, "message": "DocType WMS Stock Balance not found"}

    meta = frappe.get_meta(DT)
    filters = {"item_code": item_code}
    if company and meta.has_field("company"):
        filters["company"] = company
    if warehouse and meta.has_field("warehouse"):
        filters["warehouse"] = warehouse

    fields = ["location", "carton", "qty", "reserved_qty", "last_txn_datetime"]
    if meta.has_field("warehouse"):
        fields = ["warehouse"] + fields

    try:
        rows = frappe.get_all(
            DT,
            filters=filters,
            fields=fields,
            order_by="location asc, carton asc",
            limit_page_length=limit,
        )
    except Exception as e:
        return {"ok": False, "rows": [], "count": 0, "message": cstr(e)}

    out = []
    for r in rows:
        qty = flt(r.get("qty"), 2)
        reserved = flt(r.get("reserved_qty"), 2)
        out.append({
            "location": r.get("location"),
            "carton": r.get("carton"),
            "warehouse": r.get("warehouse"),
            "balance": qty,
            "reserved": reserved,
            "available": qty - reserved,
            "last_txn_datetime": r.get("last_txn_datetime"),
        })

    return {"ok": True, "rows": out, "count": len(out)}
