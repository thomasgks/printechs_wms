# -*- coding: utf-8 -*-
"""
ASN import UI helpers (e.g. options for warehouse, currency). Expand as needed.
"""
from __future__ import annotations

import frappe


@frappe.whitelist()
def get_warehouse_code(warehouse_name):
    """Return Warehouse code for a given name (for import UI)."""
    if not warehouse_name:
        return None
    return frappe.db.get_value("Warehouse", warehouse_name, "code")
