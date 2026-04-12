# -*- coding: utf-8 -*-
"""
WMS sync APIs for Desktop: ASN list and Transfer Order list.

Key points (Full version):
- Uses get_all for list endpoints (fast).
- Safe field picking: returns only fields that exist (avoids "Unknown column" errors).
- ASN list: forces docstatus=1 (submitted only) by default.
- ASN items: finds correct parentfield automatically and fetches child rows via get_all.
- ASN status update: prevents Select validation error (e.g., "Exported") by mapping to allowed ERP status
  and saving the external status in wms_status/sync_status/note if available.
- Transfer Order list: returns header + items (via child doctype get_all).
- Transfer Order detail: full header + items, safe getters, includes store/store_code if present.

PUT THIS FILE AS:
apps/printechs_wms/printechs_wms/api/wms_sync.py
(or wherever your hooks point to: printechs_wms.api.wms_sync)
"""

from __future__ import annotations

import json
import frappe
from frappe import _
from frappe.utils import cstr, cint


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def _parse_json_if_str(val, default):
    if val is None:
        return default
    if isinstance(val, str):
        try:
            return json.loads(val)
        except Exception:
            return default
    return val


def _get_meta_safe(doctype: str):
    """Frappe versions differ: get_meta(ignore_permissions=...) may not exist."""
    try:
        return frappe.get_meta(doctype, ignore_permissions=True)
    except TypeError:
        return frappe.get_meta(doctype)


def _pick_existing_fields(doctype: str, wanted_fields: list[str]) -> list[str]:
    """
    Return only fields that exist in DocType meta.
    Also keeps system columns: name, owner, creation, modified, modified_by, docstatus, idx.
    """
    meta = _get_meta_safe(doctype)

    system_fields = {
        "name",
        "owner",
        "creation",
        "modified",
        "modified_by",
        "docstatus",
        "idx",
    }

    out: list[str] = []
    for f in wanted_fields:
        if f in system_fields or (meta and meta.has_field(f)):
            out.append(f)

    if "name" not in out:
        out.insert(0, "name")

    return out


def _get_child_table_fieldname(parent_dt: str, child_dt: str) -> str | None:
    """
    Find the fieldname of the child table in parent doctype that points to child_dt.
    Example: parent WMS ASN has table field 'items' with options 'WMS ASN Item'
    """
    meta = frappe.get_meta(parent_dt)
    for df in (meta.fields or []):
        if df.fieldtype == "Table" and df.options == child_dt:
            return df.fieldname
    return None


def _get_select_options(doctype: str, fieldname: str) -> list[str] | None:
    """Return list of Select options if field is Select. Otherwise None."""
    try:
        meta = frappe.get_meta(doctype)
        df = meta.get_field(fieldname) if meta else None
        if not df:
            return None
        if df.fieldtype != "Select":
            return None
        opts = df.options or ""
        options = [x.strip() for x in opts.split("\n") if x.strip()]
        return options or None
    except Exception:
        return None


def _safe_set(doc, fieldname: str, value) -> bool:
    """Set field only if exists in DocType meta (or is a standard property like status)."""
    try:
        meta = frappe.get_meta(doc.doctype)
        existing = {df.fieldname for df in (meta.fields or [])}
        existing.update({"status"})  # common field
        if fieldname in existing:
            setattr(doc, fieldname, value)
            return True
    except Exception:
        pass
    return False


def _append_note(doc, fieldname: str, msg: str) -> bool:
    """Append text into note-like fields if they exist."""
    if not msg:
        return False
    try:
        meta = frappe.get_meta(doc.doctype)
        existing = {df.fieldname for df in (meta.fields or [])}
        if fieldname in existing:
            cur = cstr(doc.get(fieldname) or "")
            if cur:
                cur = cur.rstrip() + "\n"
            doc.set(fieldname, cur + msg)
            return True
    except Exception:
        pass
    return False


# ---------------------------------------------------------------------
# ASN APIs
# ---------------------------------------------------------------------

@frappe.whitelist()
def get_asns_for_wms(filters=None, fields=None, include_items: int = 0, item_fields=None):
    """
    Return list of submitted WMS ASNs for sync.

    params:
      filters: dict or JSON string
      fields: optional list/tuple or JSON string list to override (still filtered by meta)
      include_items: 0/1 - if 1, embed items list per ASN (slower)
      item_fields: override fields for items (list or JSON string list)
    """
    filters = _parse_json_if_str(filters, {}) or {}

    # IMPORTANT: only submitted
    filters["docstatus"] = 1

    wanted = [
        "name",
        "status",
        "company",
        "supplier",
        "posting_date",
        "expected_arrival",
        "shipment_date",
        "modified",
        "shipment_type",
        "airway_bill",
        "default_receiving_warehouse",
        "default_receiving_warehouse_code",
    ]

    override = _parse_json_if_str(fields, None)
    if isinstance(override, (list, tuple)) and override:
        wanted = list(override)

    list_fields = _pick_existing_fields("WMS ASN", wanted)

    asns = frappe.get_all(
        "WMS ASN",
        filters=filters,
        fields=list_fields,
        order_by="modified desc",
    )

    if int(include_items or 0) == 1 and asns:
        wanted_item_fields = _parse_json_if_str(item_fields, None)
        for row in asns:
            row["items"] = get_asn_items_for_wms(
                asn_name=row.get("name"),
                fields=wanted_item_fields
            )["items"]

    return {"ok": True, "count": len(asns), "asns": asns}


@frappe.whitelist()
def get_asn_items_for_wms(asn_name=None, fields=None):
    """
    Return child items for a given ASN.

    params:
      asn_name: required
      fields: optional list/tuple or JSON string list (still meta-filtered)
    """
    if not asn_name:
        asn_name = frappe.local.form_dict.get("asn_name")
    asn_name = cstr(asn_name).strip()

    if not asn_name:
        return {"ok": False, "items": [], "count": 0, "message": "asn_name is required"}

    parent_dt = "WMS ASN"
    child_dt = "WMS ASN Item"

    child_fieldname = _get_child_table_fieldname(parent_dt, child_dt)
    if not child_fieldname:
        return {
            "ok": False,
            "items": [],
            "count": 0,
            "message": (
                f"No Table field found in '{parent_dt}' for child '{child_dt}'. "
                f"Check the child table field in WMS ASN."
            ),
        }

    wanted = [
        "name",
        "item_code",
        "item_name",
        "uom",

        # quantities (include multiple possible names; meta-filter will pick existing)
        "qty",
        "shipped_qty",
        "asn_qty",
        "ordered_qty",
        "planned_qty",

        "received_qty",
        "pending_qty",

        "carton_id",

        # optional commercial fields
        "rate",
        "amount",
        "barcode",

        "idx",
        "modified",
    ]

    override = _parse_json_if_str(fields, None)
    if isinstance(override, (list, tuple)) and override:
        wanted = list(override)

    item_list_fields = _pick_existing_fields(child_dt, wanted)

    items = frappe.get_all(
        child_dt,
        filters={
            "parenttype": parent_dt,
            "parent": asn_name,
            "parentfield": child_fieldname,
        },
        fields=item_list_fields,
        order_by="idx asc",
    )

    return {"ok": True, "asn_name": asn_name, "count": len(items), "items": items}


@frappe.whitelist()
def get_asns_for_wms_sync(filters=None):
    """Backward-compatible alias."""
    return get_asns_for_wms(filters=filters)


@frappe.whitelist()
def update_asn_wms_status(asn_name=None, status=None, wms_status=None, wms_reference=None, note=None):
    """
    Update ASN status from WMS/Desktop safely.

    Desktop may send "Exported", but ERP select field 'status' only allows:
      Draft, Open, Completed, Cancelled

    Strategy:
    - If incoming status is not allowed, map it to allowed ERP status.
    - Store original incoming status in wms_status/sync_status/note if present.
    - Do NOT set doc.status to "Open" when doc is submitted (e.g. Received)
      to avoid UpdateAfterSubmitError.
    """
    if not asn_name:
        asn_name = frappe.local.form_dict.get("asn_name")
    if status is None:
        status = frappe.local.form_dict.get("status")
    if wms_status is None:
        wms_status = frappe.local.form_dict.get("wms_status")
    if wms_reference is None:
        wms_reference = frappe.local.form_dict.get("wms_reference")
    if note is None:
        note = frappe.local.form_dict.get("note")

    asn_name = cstr(asn_name).strip()
    incoming = cstr(status or wms_status).strip()

    if not asn_name:
        return {"ok": False, "message": "asn_name is required"}
    if not incoming:
        return {"ok": False, "message": "status (or wms_status) is required"}

    if not frappe.db.exists("WMS ASN", asn_name):
        return {"ok": False, "message": f"WMS ASN '{asn_name}' not found"}

    doc = frappe.get_doc("WMS ASN", asn_name)

    allowed_status = _get_select_options("WMS ASN", "status")

    # External -> ERP mapping (adjust as needed)
    map_to_erp = {
        "Exported": "Open",
        "Synced": "Open",
        "In Progress": "Open",
        "Received": "Completed",
        "Completed": "Completed",
        "Cancelled": "Cancelled",
        "Canceled": "Cancelled",
        "Open": "Open",
        "Draft": "Draft",
    }

    erp_status = incoming
    mapped = False

    if allowed_status:
        if incoming not in allowed_status:
            erp_status = map_to_erp.get(incoming) or "Open"
            mapped = True
            if erp_status not in allowed_status:
                erp_status = "Open" if "Open" in allowed_status else allowed_status[0]

    # Save original external status somewhere if we mapped
    external_saved_to = None
    if mapped:
        if _safe_set(doc, "wms_status", incoming):
            external_saved_to = "wms_status"
        elif _safe_set(doc, "sync_status", incoming):
            external_saved_to = "sync_status"
        elif _append_note(doc, "wms_note", f"WMS Status: {incoming}"):
            external_saved_to = "wms_note"
        elif _append_note(doc, "note", f"WMS Status: {incoming}"):
            external_saved_to = "note"

    # Set ERP status (only if field exists)
    # Do NOT set status to "Open" for submitted docs (Received -> Open triggers UpdateAfterSubmitError)
    erp_status_set = None
    if cint(doc.docstatus) == 1 and erp_status == "Open":
        # Desktop sent "Exported" -> mapped to "Open"; skip changing status on submitted ASN
        pass
    elif _safe_set(doc, "status", erp_status):
        erp_status_set = erp_status
    else:
        # if no status field exists, try wms_status
        _safe_set(doc, "wms_status", incoming)

    # Optional fields (desktop sends wms_ref)
    wms_ref = frappe.local.form_dict.get("wms_ref") or wms_reference
    if wms_ref:
        _safe_set(doc, "wms_reference", wms_ref)

    if note:
        if not _safe_set(doc, "wms_note", note):
            _safe_set(doc, "note", note)

    doc.flags.ignore_permissions = True
    doc.save()

    return {
        "ok": True,
        "asn_name": asn_name,
        "incoming_status": incoming,
        "erp_status_set": erp_status_set,
        "allowed_status": allowed_status,
        "external_status_saved_to": external_saved_to,
        "modified": doc.modified,
    }


# ---------------------------------------------------------------------
# Transfer Order APIs
# ---------------------------------------------------------------------

@frappe.whitelist()
def get_tos_for_wms(filters=None):
    """
    Return list of SUBMITTED WMS Transfer Orders with items for WMS sync
    """

    filters = _parse_json_if_str(filters, {}) or {}

    # ✅ only submitted TO
    filters["docstatus"] = 1

    # ---------------------------------------------------
    # HEADER
    # ---------------------------------------------------
    to_header_wanted = [
        "name",
        "to_title",
        "asn",
        "from_warehouse_code",
        "from_warehouse",
        "to_warehouse_code",
        "to_warehouse",
        "status",
        "required_date",
        "modified",
        "docstatus",
    ]

    to_header_fields = _pick_existing_fields(
        "WMS Transfer Order",
        to_header_wanted
    )

    tos = frappe.get_all(
        "WMS Transfer Order",
        filters=filters,
        fields=to_header_fields,
        order_by="modified desc",
    )

    # ---------------------------------------------------
    # ITEMS  ✅ ADD STORE HERE
    # ---------------------------------------------------
    to_item_wanted = [
        "name",
        "store",            # ✅ THIS WAS MISSING
        "item_code",
        "qty",
        "allocated_qty",
        "idx",
        "modified",
    ]

    to_item_fields = _pick_existing_fields(
        "WMS Transfer Order Item",
        to_item_wanted
    )

    # ---------------------------------------------------
    # ATTACH ITEMS
    # ---------------------------------------------------
    for to_doc in tos:
        to_name = to_doc.get("name")

        items = frappe.get_all(
            "WMS Transfer Order Item",
            filters={
                "parent": to_name,
                "parenttype": "WMS Transfer Order"
            },
            fields=to_item_fields,
            order_by="idx asc",
        )

        to_doc["items"] = items

    return {
        "ok": True,
        "count": len(tos),
        "tos": tos
    }

@frappe.whitelist(allow_guest=False)
def get_transfer_order_detail(to_name: str) -> dict:
    """
    URL:
      /api/method/printechs_wms.api.wms_sync.get_transfer_order_detail?to_name=WMS-TO-00001

    Returns:
      header fields + items list (includes store / store_code if available)
    """
    if not to_name:
        frappe.throw(_("to_name is required"))

    TO_DOCTYPE = "WMS Transfer Order"
    TO_ITEM_DOCTYPE = "WMS Transfer Order Item"

    if not frappe.db.exists(TO_DOCTYPE, to_name):
        frappe.throw(_("{0} not found: {1}").format(TO_DOCTYPE, to_name))

    doc = frappe.get_doc(TO_DOCTYPE, to_name)

    def _get(doc_obj, fieldname, default=None):
        try:
            return getattr(doc_obj, fieldname)
        except Exception:
            return default

    header = {
        "to_name": doc.name,
        "docstatus": cint(doc.docstatus),
        "status": _get(doc, "status"),
        "company": _get(doc, "company"),
        "posting_date": _get(doc, "posting_date"),
        "transaction_date": _get(doc, "transaction_date"),
        "remarks": _get(doc, "remarks"),

        "from_warehouse": _get(doc, "from_warehouse") or _get(doc, "source_warehouse"),
        "to_warehouse": _get(doc, "to_warehouse") or _get(doc, "target_warehouse"),

        "from_store": _get(doc, "from_store") or _get(doc, "from_store_code") or _get(doc, "store"),
        "to_store": _get(doc, "to_store") or _get(doc, "to_store_code"),
    }

    items_out = []
    items = getattr(doc, "items", None) or []

    for row in items:
        def _getr(fieldname, default=None):
            try:
                return getattr(row, fieldname)
            except Exception:
                return default

        item = {
            "name": row.name,
            "idx": row.idx,

            "item_code": _getr("item_code"),
            "item_name": _getr("item_name"),
            "uom": _getr("uom"),
            "qty": float(_getr("qty", 0) or 0),

            # store/store_code requirement
            "store": _getr("store") or _getr("store_code") or _getr("branch") or _getr("site"),
            "store_code": _getr("store_code") or _getr("store") or _getr("branch_code"),

            # optional logistics fields
            "from_location_id": _getr("from_location_id"),
            "to_location_id": _getr("to_location_id"),
            "carton_id": _getr("carton_id"),
            "batch_no": _getr("batch_no"),
            "serial_no": _getr("serial_no"),

            # costing if exists
            "rate": float(_getr("rate", 0) or 0),
            "amount": float(_getr("amount", 0) or 0),
        }
        items_out.append(item)

    return {
        "ok": True,
        "doctype": TO_DOCTYPE,
        "header": header,
        "items": items_out,
        "items_count": len(items_out),
    }