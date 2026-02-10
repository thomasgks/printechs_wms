# -*- coding: utf-8 -*-
from __future__ import annotations

import frappe
from frappe import _


def _flt(v):
    try:
        return float(v or 0)
    except Exception:
        return 0.0


def _resolve_warehouse(name: str | None, code: str | None) -> str:
    """
    Returns Warehouse.name that exists in ERPNext.
    Priority:
      1) If name is given and exists -> return it
      2) If code is given -> find Warehouse where code == code
    """
    if name and frappe.db.exists("Warehouse", name):
        return name

    if code:
        wh = frappe.db.get_value("Warehouse", {"code": code}, "name")
        if wh:
            return wh

    frappe.throw(_("Warehouse not found. Provide valid warehouse name or code. name={0}, code={1}")
                .format(name or "-", code or "-"))


@frappe.whitelist(methods=["POST"])
def create_stock_entry_from_transfer_carton(payload=None):
    """
    Desktop Transfer Carton ? ERPNext Stock Entry (Material Transfer)

    Accepts warehouse by:
      - from_warehouse (ERPNext name) OR from_warehouse_code (Warehouse.code)
      - to_warehouse (ERPNext name) OR to_warehouse_code (Warehouse.code)
    """

    if payload is None:
        payload = frappe.form_dict.get("payload")

    if isinstance(payload, str):
        import json
        payload = json.loads(payload)

    if not isinstance(payload, dict):
        frappe.throw(_("payload must be a JSON object"))

    # Desktop references
    transfer_carton_id = payload.get("transfer_carton_id")
    asn_no = payload.get("asn_no")
    transfer_order = payload.get("transfer_order")

    company = payload.get("company")
    posting_date = payload.get("posting_date")
    submit_flag = int(payload.get("submit") or 1)

    # Warehouses - accept name or code
    from_wh_name = payload.get("from_warehouse")
    to_wh_name = payload.get("to_warehouse")
    from_wh_code = payload.get("from_warehouse_code")
    to_wh_code = payload.get("to_warehouse_code")

    items = payload.get("items") or []

    if not company:
        frappe.throw(_("company is required"))
    if not items:
        frappe.throw(_("items cannot be empty"))

    # Resolve to real ERPNext warehouse names
    from_wh = _resolve_warehouse(from_wh_name, from_wh_code)
    to_wh = _resolve_warehouse(to_wh_name, to_wh_code)

    se = frappe.new_doc("Stock Entry")
    se.stock_entry_type = "Material Transfer"
    se.company = company
    se.from_warehouse = from_wh
    se.to_warehouse = to_wh

    if posting_date:
        se.posting_date = posting_date

    for row in items:
        row = row or {}
        item_code = row.get("item_code")
        qty = _flt(row.get("qty"))
        uom = row.get("uom")
        source_carton = row.get("source_carton")

        if not item_code:
            frappe.throw(_("Item Code missing in item row"))
        if qty <= 0:
            frappe.throw(_("Invalid quantity for item {0}").format(item_code))

        se.append("items", {
            "item_code": item_code,
            "qty": qty,
            "uom": uom,
            "s_warehouse": from_wh,
            "t_warehouse": to_wh,
            # optional custom field (if you create it on Stock Entry Detail)
            # "source_carton": source_carton
        })

    se.insert(ignore_permissions=True)

    if submit_flag:
        se.submit()

    return {
        "ok": True,
        "stock_entry_no": se.name,
        "docstatus": se.docstatus,
        "submitted": bool(se.docstatus == 1),
        "resolved": {
            "from_warehouse": from_wh,
            "to_warehouse": to_wh
        },
        "reference": {
            "transfer_carton_id": transfer_carton_id,
            "asn_no": asn_no,
            "transfer_order": transfer_order
        }
    }
