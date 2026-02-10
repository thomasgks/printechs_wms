# -*- coding: utf-8 -*-
"""
ASN receiving: update received qty from WMS Desktop. DB-only updates (no get_doc).
"""
from __future__ import annotations

import frappe


@frappe.whitelist()
def update_asn_received_qty(asn_name, item_code, received_qty, child_name=None):
    """
    Update received qty for an ASN item. Uses DB only for performance.
    asn_name: WMS ASN name
    item_code: Item code
    received_qty: new received qty value
    child_name: optional WMS ASN Item doc name; if not set, found by parent + item_code
    """
    if not asn_name or item_code is None:
        frappe.throw("asn_name and item_code are required")
    try:
        received_qty = float(received_qty)
    except (TypeError, ValueError):
        frappe.throw("received_qty must be a number")
    if child_name:
        child = child_name
    else:
        child = frappe.db.get_value(
            "WMS ASN Item",
            {"parent": asn_name, "item_code": item_code},
            "name",
        )
    if not child:
        frappe.throw(f"WMS ASN Item not found for ASN={asn_name}, item_code={item_code}")
    meta = frappe.get_meta("WMS ASN Item", ignore_permissions=True)
    if not meta.has_field("received_qty"):
        frappe.throw("WMS ASN Item has no field received_qty")
    frappe.db.set_value("WMS ASN Item", child, "received_qty", received_qty, update_modified=True)
    frappe.db.commit()
    return {"ok": True, "name": child}
