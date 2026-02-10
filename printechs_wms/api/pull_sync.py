# -*- coding: utf-8 -*-
"""
Pull sync: get ASN detail for WMS Desktop (e.g. default receiving warehouse).
"""
from __future__ import annotations

import frappe


@frappe.whitelist()
def get_asn_detail(name):
    """
    Return ASN detail for sync. Includes default_receiving_warehouse_code and default_receiving_warehouse_name.
    Uses get_value to avoid loading full doc.
    """
    if not name:
        return None
    out = {"name": name}
    meta = frappe.get_meta("WMS ASN", ignore_permissions=True)
    fields = ["status", "company", "supplier", "posting_date", "default_receiving_warehouse", "default_receiving_warehouse_code"]
    for f in fields:
        if meta.has_field(f):
            val = frappe.db.get_value("WMS ASN", name, f)
            out[f] = val
    if out.get("default_receiving_warehouse"):
        out["default_receiving_warehouse_name"] = frappe.db.get_value("Warehouse", out["default_receiving_warehouse"], "name") or out["default_receiving_warehouse"]
    if out.get("default_receiving_warehouse_code") and not out.get("default_receiving_warehouse_name"):
        wh = frappe.db.get_value("Warehouse", {"code": out["default_receiving_warehouse_code"]}, "name")
        if wh:
            out["default_receiving_warehouse_name"] = wh
    return out
