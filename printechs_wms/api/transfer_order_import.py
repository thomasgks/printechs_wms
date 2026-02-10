# -*- coding: utf-8 -*-
"""
Transfer Order Excel import. Stub so WMS Transfer Order form does not break.
Implement import logic as needed (read Excel from file_url, create/update TO docs).
"""
from __future__ import annotations

import frappe


@frappe.whitelist()
def import_transfer_order_from_excel(file_url=None, sheet_name=None, create_new=1, submit=0):
    """
    Import Transfer Order(s) from an Excel file.
    Args: file_url, sheet_name, create_new (1=create new, 0=overwrite), submit (0/1).
    Returns: dict with count, errors, first_to (name of first TO).
    """
    # Stub: implement actual Excel read and TO creation/update here
    return {
        "count": 0,
        "errors": ["Transfer Order import not implemented. Add logic in transfer_order_import.import_transfer_order_from_excel."],
        "first_to": None,
    }
