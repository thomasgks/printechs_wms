# -*- coding: utf-8 -*-
from __future__ import annotations

import frappe
from frappe.utils import cstr, flt


@frappe.whitelist()
def get_item_location_carton_balance(item_code=None, warehouse=None, company=None, limit=500):
    if not item_code or not cstr(item_code).strip():
        return {"ok": False, "rows": [], "count": 0, "message": "item_code is required"}

    DT = "WMS Stock Balance"
    if not frappe.db.exists("DocType", DT):
        return {"ok": False, "rows": [], "count": 0, "message": f"DocType {DT} not found"}

    item_code = cstr(item_code).strip()
    warehouse = cstr(warehouse).strip() if warehouse else None
    company = cstr(company).strip() if company else None
    limit = int(limit or 500)

    meta = frappe.get_meta(DT)

    # --- detect fieldnames ---
    item_field = "item_code" if meta.has_field("item_code") else ("item" if meta.has_field("item") else None)
    wh_field = "warehouse" if meta.has_field("warehouse") else ("warehouse_code" if meta.has_field("warehouse_code") else None)
    loc_field = "location" if meta.has_field("location") else ("bin_location" if meta.has_field("bin_location") else ("location_id" if meta.has_field("location_id") else None))
    carton_field = "carton" if meta.has_field("carton") else ("carton_id" if meta.has_field("carton_id") else None)
    qty_field = "qty" if meta.has_field("qty") else ("balance_qty" if meta.has_field("balance_qty") else ("actual_qty" if meta.has_field("actual_qty") else None))
    res_field = "reserved_qty" if meta.has_field("reserved_qty") else ("reserved" if meta.has_field("reserved") else None)
    dt_field = "last_txn_datetime" if meta.has_field("last_txn_datetime") else ("modified" if meta.has_field("modified") else None)

    if not item_field or not qty_field:
        return {
            "ok": False,
            "rows": [],
            "count": 0,
            "message": f"Missing required fields. Detected item_field={item_field}, qty_field={qty_field}. Please check DocType fields."
        }

    # --- build filters ---
    filters = {item_field: item_code}

    if company and meta.has_field("company"):
        filters["company"] = company

    # warehouse handling:
    # If wh_field is warehouse_code, UI may pass ERP warehouse name -> try mapping if you have it,
    # otherwise try both direct match and 'code' match.
    if warehouse and wh_field:
        filters[wh_field] = warehouse

    # --- fields to fetch ---
    fields = []
    for f in [wh_field, loc_field, carton_field, qty_field, res_field, dt_field]:
        if f and f not in fields:
            fields.append(f)

    # if some are missing, still run (don’t crash)
    try:
        rows = frappe.get_all(
            DT,
            filters=filters,
            fields=fields,
            order_by=f"{(loc_field or 'name')} asc, {(carton_field or 'name')} asc",
            limit_page_length=limit,
            ignore_permissions=True,   # remove if you want permission-based filtering
        )
    except Exception as e:
        return {"ok": False, "rows": [], "count": 0, "message": cstr(e)}

    out = []
    for r in rows:
        qty = flt(r.get(qty_field), 2)
        reserved = flt(r.get(res_field), 2) if res_field else 0.0
        out.append({
            "location": r.get(loc_field) if loc_field else None,
            "carton": r.get(carton_field) if carton_field else None,
            "warehouse": r.get(wh_field) if wh_field else None,
            "balance": qty,
            "reserved": reserved,
            "available": qty - reserved,
            "last_txn_datetime": r.get(dt_field) if dt_field else None,
        })

    return {
        "ok": True,
        "rows": out,
        "count": len(out),
        "detected_fields": {
            "item_field": item_field,
            "warehouse_field": wh_field,
            "location_field": loc_field,
            "carton_field": carton_field,
            "qty_field": qty_field,
            "reserved_field": res_field,
            "datetime_field": dt_field,
        },
    }
