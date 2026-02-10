# -*- coding: utf-8 -*-
"""
ASN import from Excel. Supports currency, conversion_rate, default_receiving_warehouse_code/name.
Implement parse logic and doc creation as needed.
"""
from __future__ import annotations

import frappe


@frappe.whitelist()
def import_asn_from_excel(file_url=None, sheet_name=None, default_company=None, default_receiving_warehouse_code=None, default_receiving_warehouse=None, currency=None, conversion_rate=None):
    """
    Import ASN(s) from Excel. Optional: default_company, default_receiving_warehouse_code/name, currency, conversion_rate.
    Returns: { "count": n, "names": [...], "errors": [...] }
    """
    # Stub: implement Excel read and WMS ASN creation here
    return {
        "count": 0,
        "names": [],
        "errors": ["ASN import not implemented. Add logic in asn_import.import_asn_from_excel."],
    }
