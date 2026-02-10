# -*- coding: utf-8 -*-
"""
WMS sync APIs for Desktop: ASN list and Transfer Order list.
Uses get_all (no get_doc) for performance. Returns shipment_type, airway_bill for ASNs; items for TOs.
"""
from __future__ import annotations

import frappe


@frappe.whitelist()
def get_asns_for_wms(filters=None, fields=None):
    """
    Return list of WMS ASNs for sync. Includes shipment_type, airway_bill when available.
    filters: optional dict (e.g. {"status": "Open"}).
    """
    if filters is None:
        filters = {}
    if isinstance(filters, str):
        import json
        try:
            filters = json.loads(filters)
        except Exception:
            filters = {}
    list_fields = ["name", "status", "company", "supplier", "posting_date", "modified"]
    optional = ["shipment_type", "airway_bill", "default_receiving_warehouse", "default_receiving_warehouse_code"]
    meta = frappe.get_meta("WMS ASN", ignore_permissions=True)
    for f in optional:
        if meta.has_field(f):
            list_fields.append(f)
    if fields:
        list_fields = fields if isinstance(fields, (list, tuple)) else list_fields
    try:
        asns = frappe.get_all(
            "WMS ASN",
            filters=filters,
            fields=list_fields,
            order_by="modified desc",
        )
    except Exception:
        asns = frappe.get_all(
            "WMS ASN",
            filters=filters,
            fields=["name", "status", "company", "modified"],
            order_by="modified desc",
        )
    return asns


@frappe.whitelist()
def get_asns_for_wms_sync(filters=None):
    """Alias for get_asns_for_wms."""
    return get_asns_for_wms(filters=filters)


@frappe.whitelist()
def get_tos_for_wms(filters=None):
    """
    Return list of WMS Transfer Orders with items (child table) for sync.
    """
    if filters is None:
        filters = {}
    if isinstance(filters, str):
        import json
        try:
            filters = json.loads(filters)
        except Exception:
            filters = {}
    tos = frappe.get_all(
        "WMS Transfer Order",
        filters=filters,
        fields=["name", "to_title", "asn", "from_warehouse_code", "from_warehouse", "status", "required_date", "modified"],
        order_by="modified desc",
    )
    for to_doc in tos:
        try:
            items = frappe.get_all(
                "WMS Transfer Order Item",
                filters={"parent": to_doc.name},
                fields=["item_code", "qty", "allocated_qty", "idx"],
                order_by="idx",
            )
            to_doc["items"] = items
        except Exception:
            to_doc["items"] = []
    return tos
