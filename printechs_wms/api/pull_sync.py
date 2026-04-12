# -*- coding: utf-8 -*-
"""
Pull sync: APIs for WMS Desktop to pull ASN headers/details from ERPNext.

Endpoints (examples):
- /api/method/printechs_wms.api.pull_sync.get_asns_for_wms?modified_after=2025-01-01%2000:00:00&limit=50
- /api/method/printechs_wms.api.pull_sync.get_asn_detail?name=ASN-0003
"""
from __future__ import annotations

import frappe
from frappe.utils import cint


ASN_DOCTYPE = "WMS ASN"


def _has_field(doctype: str, fieldname: str) -> bool:
    try:
        meta = frappe.get_meta(doctype, ignore_permissions=True)
        return bool(meta and meta.has_field(fieldname))
    except Exception:
        return False


def _warehouse_display_name(warehouse_docname: str | None) -> str | None:
    """Return a nice warehouse label; fallback to docname."""
    if not warehouse_docname:
        return None
    # ERPNext Warehouse usually has warehouse_name
    wh_name = frappe.db.get_value("Warehouse", warehouse_docname, "warehouse_name")
    return wh_name or warehouse_docname


def _warehouse_docname_from_code(code: str | None) -> str | None:
    """Find Warehouse docname by custom code field (your field is 'code')."""
    if not code:
        return None
    return frappe.db.get_value("Warehouse", {"code": code}, "name")


@frappe.whitelist()
def get_asn_detail(name: str):
    """
    Return ASN detail for sync. Includes default_receiving_warehouse_code and default_receiving_warehouse_name.
    Uses get_value to avoid loading the full document.
    """
    if not name:
        return {"ok": False, "error": "name is required"}

    if not frappe.db.exists(ASN_DOCTYPE, name):
        return {"ok": False, "error": f"{ASN_DOCTYPE} not found", "name": name}

    out = {"ok": True, "name": name}

    fields = [
        "status",
        "docstatus",
        "company",
        "supplier",
        "posting_date",
        "modified",
        "default_receiving_warehouse",
        "default_receiving_warehouse_code",
        "purchase_receipt",
        "wms_export_status",
    ]

    for f in fields:
        if _has_field(ASN_DOCTYPE, f):
            out[f] = frappe.db.get_value(ASN_DOCTYPE, name, f)

    # Resolve warehouse display name
    wh = out.get("default_receiving_warehouse")
    wh_code = out.get("default_receiving_warehouse_code")

    if wh:
        out["default_receiving_warehouse_name"] = _warehouse_display_name(wh)
    else:
        out["default_receiving_warehouse_name"] = None

    # If warehouse docname missing but code exists, try to resolve by code
    if (not wh) and wh_code:
        wh_doc = _warehouse_docname_from_code(wh_code)
        if wh_doc:
            out["default_receiving_warehouse"] = wh_doc
            out["default_receiving_warehouse_name"] = _warehouse_display_name(wh_doc)

    return out


@frappe.whitelist()
def get_asns_for_wms(
    modified_after: str | None = None,
    limit: int | str = 50,
    include_details: int | str = 0,
):
    """
    List ASNs modified after a given timestamp for WMS Desktop pull.

    Query params:
      - modified_after: "YYYY-MM-DD HH:MM:SS" (optional)
      - limit: max rows (default 50, capped)
      - include_details: 0/1 (if 1, will call get_asn_detail for each row)

    Returns:
      {
        "ok": true,
        "filters": {...},
        "count": N,
        "data": [...]
      }
    """
    lim = max(1, min(500, cint(limit or 50)))  # hard-cap to avoid heavy pulls
    inc = cint(include_details or 0)

    filters = {}

    # Use modified filter if provided
    if modified_after:
        filters["modified"] = (">", modified_after)

    # Optional: pull only submitted/draft etc. (enable if you want)
    # Example: only submitted
    # if _has_field(ASN_DOCTYPE, "docstatus"):
    #     filters["docstatus"] = 1

    # Fields to return (only if exist)
    base_fields = ["name", "modified"]
    optional_fields = [
        "status",
        "docstatus",
        "company",
        "supplier",
        "posting_date",
        "default_receiving_warehouse",
        "default_receiving_warehouse_code",
        "purchase_receipt",
        "wms_export_status",
    ]

    fields = []
    for f in base_fields + optional_fields:
        if f in ("name", "modified") or _has_field(ASN_DOCTYPE, f):
            fields.append(f)

    rows = frappe.get_all(
        ASN_DOCTYPE,
        filters=filters,
        fields=fields,
        order_by="modified asc",
        limit_page_length=lim,
    )

    data = []
    if inc:
        # include_details=1 => enrich each row with resolved warehouse name etc.
        for r in rows:
            data.append(get_asn_detail(r.get("name")))
    else:
        # lightweight response + warehouse name resolution (no extra DB loads if possible)
        for r in rows:
            r = dict(r)
            if r.get("default_receiving_warehouse"):
                r["default_receiving_warehouse_name"] = _warehouse_display_name(
                    r["default_receiving_warehouse"]
                )
            elif r.get("default_receiving_warehouse_code"):
                wh_doc = _warehouse_docname_from_code(r["default_receiving_warehouse_code"])
                r["default_receiving_warehouse_name"] = _warehouse_display_name(wh_doc) if wh_doc else None
            else:
                r["default_receiving_warehouse_name"] = None
            data.append(r)

    return {
        "ok": True,
        "filters": {
            "modified_after": modified_after,
            "limit": lim,
            "include_details": inc,
        },
        "count": len(data),
        "data": data,
    }
