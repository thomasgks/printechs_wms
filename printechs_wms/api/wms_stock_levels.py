# -*- coding: utf-8 -*-
from __future__ import annotations
import frappe

@frappe.whitelist()
def get_item_wms_stock_grouped(item_code: str, company: str | None = None):
    if not item_code:
        frappe.throw("item_code is required")

    filters = {"item_code": item_code}
    if company:
        filters["company"] = company

    rows = frappe.get_all(
        "WMS Stock Balance",
        filters=filters,
        fields=["location", "carton", "qty", "reserved_qty", "last_txn_datetime"],
        order_by="location asc, carton asc",
        limit_page_length=1000,
    )

    grouped = {}
    for r in rows:
        loc = r.get("location") or "NO-LOCATION"
        grouped.setdefault(loc, {"location": loc, "total_qty": 0.0, "rows": []})
        grouped[loc]["total_qty"] += float(r.get("qty") or 0)
        grouped[loc]["rows"].append(r)

    data = [grouped[k] for k in sorted(grouped.keys())]
    return {"ok": True, "data": data, "count": len(rows)}

