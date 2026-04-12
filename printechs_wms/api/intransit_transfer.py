# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import frappe
from frappe import _
from frappe.utils import nowdate, nowtime, getdate

API_VERSION = "1.0.0"

# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------
def _cstr(x) -> str:
    if x is None:
        return ""
    if isinstance(x, str):
        return x.strip()
    return str(x).strip()


def _as_int(v, default=0) -> int:
    try:
        if v is None:
            return int(default)
        if isinstance(v, bool):
            return 1 if v else 0
        s = str(v).strip().lower()
        if s in ("true", "yes", "y", "1", "t", "on"):
            return 1
        if s in ("false", "no", "n", "0", "f", "off"):
            return 0
        return int(float(s))
    except Exception:
        return int(default)


def _safe_json_body():
    """Reads JSON body if present. Returns dict or None."""
    try:
        data = getattr(frappe.request, "data", None)  # type: ignore
        if not data:
            return None
        if isinstance(data, (bytes, bytearray)):
            data = data.decode("utf-8", "ignore")
        data = (data or "").strip()
        if not data:
            return None
        return json.loads(data)
    except Exception:
        return None


def _get_payload():
    """
    Desktop / Postman may send:
      - payload as JSON body
      - payload as form field 'payload' (stringified JSON)
      - plain form fields directly
    """
    body = _safe_json_body()
    if isinstance(body, dict):
        if isinstance(body.get("payload"), dict):
            return body["payload"]
        return body

    fd = dict(getattr(frappe, "form_dict", {}) or {})
    if "payload" in fd and fd.get("payload"):
        try:
            p = fd.get("payload")
            if isinstance(p, str):
                return frappe.parse_json(p)
            if isinstance(p, dict):
                return p
        except Exception:
            pass

    return fd


def _get_first(payload: dict, *keys, default=None):
    for k in keys:
        if k in payload and payload.get(k) not in (None, ""):
            return payload.get(k)
    return default


def _has_column(doctype: str, fieldname: str) -> bool:
    try:
        return bool(frappe.db.has_column(doctype, fieldname))
    except Exception:
        return False


def _tag_remarks(existing: str, external_ref: str) -> str:
    existing = (existing or "").strip()
    if not external_ref:
        return existing
    marker = f"[EXTREF:{external_ref}]"
    if marker in existing:
        return existing
    return (existing + "\n" + marker).strip() if existing else marker


def _ensure_receipt_link(receipt_doc, in_transit_se: str):
    """
    Store idempotency/link back to the in-transit Stock Entry.

    Preferred:
      - custom_in_transit_stock_entry (custom field on Stock Entry)
      - in_transit_stock_entry       (custom field on Stock Entry)

    Fallback:
      - tag in remarks: [InTransitSE:<name>]
    """
    if hasattr(receipt_doc, "custom_in_transit_stock_entry"):
        receipt_doc.custom_in_transit_stock_entry = in_transit_se
        return
    if hasattr(receipt_doc, "in_transit_stock_entry"):
        receipt_doc.in_transit_stock_entry = in_transit_se
        return

    tag = f"[InTransitSE:{in_transit_se}]"
    existing = (receipt_doc.remarks or "").strip()
    if tag not in existing:
        receipt_doc.remarks = (existing + "\n" + tag).strip() if existing else tag


def _find_existing_by_external_ref(external_ref: str) -> str | None:
    """Idempotency for create_material_transfer_add_to_transit."""
    if not external_ref:
        return None

    if _has_column("Stock Entry", "custom_external_ref"):
        se = frappe.db.get_value(
            "Stock Entry",
            {"custom_external_ref": external_ref, "docstatus": ["!=", 2]},
            "name",
        )
        if se:
            return se

    marker = f"[EXTREF:{external_ref}]"
    se = frappe.db.get_value(
        "Stock Entry",
        {"docstatus": ["!=", 2], "remarks": ["like", f"%{marker}%"]},
        "name",
    )
    return se or None


def _resolve_mr_item_name(material_request: str, item_code: str) -> str | None:
    """
    Auto-detect MR item row name by item_code.
    Baseline: returns first matching row.
    """
    if not material_request or not item_code:
        return None
    mr = frappe.get_doc("Material Request", material_request)
    for row in mr.items:
        if row.item_code == item_code:
            return row.name
    return None


# -------------------------
# MISSING FUNCTIONS (FIX)
# -------------------------
def _find_existing_receipt(in_transit_se: str) -> str | None:
    """
    Idempotency for end_transit_create_receipt:
    Return existing receipt Stock Entry name if already created for this in-transit SE.
    """
    if not in_transit_se:
        return None

    # Prefer explicit link fields on Stock Entry
    if _has_column("Stock Entry", "custom_in_transit_stock_entry"):
        se = frappe.db.get_value(
            "Stock Entry",
            {"custom_in_transit_stock_entry": in_transit_se, "docstatus": ["!=", 2]},
            "name",
        )
        if se:
            return se

    if _has_column("Stock Entry", "in_transit_stock_entry"):
        se = frappe.db.get_value(
            "Stock Entry",
            {"in_transit_stock_entry": in_transit_se, "docstatus": ["!=", 2]},
            "name",
        )
        if se:
            return se

    # Fallback: marker in remarks
    tag = f"[InTransitSE:{in_transit_se}]"
    se = frappe.db.get_value(
        "Stock Entry",
        {"docstatus": ["!=", 2], "remarks": ["like", f"%{tag}%"]},
        "name",
    )
    return se or None


def _detect_transit_source_warehouse(in_se_doc) -> str:
    """
    Source warehouse for receipt = the transit warehouse used in in-transit SE.
    Best effort:
      - header to_warehouse (if exists)
      - first item.t_warehouse
    """
    to_wh = _cstr(getattr(in_se_doc, "to_warehouse", ""))
    if to_wh:
        return to_wh

    try:
        if in_se_doc.items and in_se_doc.items[0].get("t_warehouse"):
            return _cstr(in_se_doc.items[0].get("t_warehouse"))
    except Exception:
        pass

    return ""


def _create_receipt_from_intransit(in_transit_se: str, receiving_warehouse: str, payload: dict):
    """
    Create receipt Stock Entry that moves stock:
      Transit Warehouse  -> Receiving Warehouse

    Uses the submitted in-transit Stock Entry as the source of items and qty.
    """
    in_se = frappe.get_doc("Stock Entry", in_transit_se)

    if int(in_se.docstatus or 0) != 1:
        frappe.throw(_("In-transit Stock Entry must be submitted: {0}").format(in_transit_se))

    # Optional sanity: must be a transfer-to-transit kind of entry
    # (do not hard-fail to keep compatibility; only validate minimum needed)
    if not in_se.items:
        frappe.throw(_("In-transit Stock Entry has no items: {0}").format(in_transit_se))

    transit_wh = _detect_transit_source_warehouse(in_se)
    if not transit_wh:
        # still can derive per row t_warehouse, but we want at least one
        transit_wh = _cstr(in_se.items[0].get("t_warehouse"))

    if not transit_wh:
        frappe.throw(_("Could not detect Transit warehouse from Stock Entry {0}").format(in_transit_se))

    # Build receipt
    receipt = frappe.new_doc("Stock Entry")
    receipt.stock_entry_type = "Material Transfer"
    receipt.company = in_se.company

    # posting date/time (default now)
    receipt.posting_date = getdate(_get_first(payload, "posting_date", default=None) or nowdate())
    receipt.posting_time = _get_first(payload, "posting_time", default=None) or nowtime()
    receipt.set_posting_time = 1

    # header warehouses where available (nice-to-have)
    if hasattr(receipt, "from_warehouse"):
        receipt.from_warehouse = transit_wh
    if hasattr(receipt, "to_warehouse"):
        receipt.to_warehouse = receiving_warehouse

    # remarks
    base_remarks = _cstr(_get_first(payload, "remarks", default="")) or _cstr(in_se.remarks)
    receipt.remarks = (base_remarks or "").strip()

    # link back for idempotency
    _ensure_receipt_link(receipt, in_transit_se)

    # Copy items (qty same as in-transit SE)
    for r in in_se.items:
        item_code = _cstr(r.get("item_code"))
        qty = float(r.get("qty") or 0)

        if not item_code or qty <= 0:
            continue

        d = receipt.append("items", {})
        d.item_code = item_code
        d.qty = qty

        # Move from Transit -> Receiving
        d.s_warehouse = transit_wh
        d.t_warehouse = receiving_warehouse

        # Optional: preserve batch/serial where possible
        if r.get("batch_no") and hasattr(d, "batch_no"):
            d.batch_no = r.get("batch_no")
        if r.get("serial_no") and hasattr(d, "serial_no"):
            d.serial_no = r.get("serial_no")

        # Optional: keep MR references if present (helps traceability)
        if r.get("material_request") and hasattr(d, "material_request"):
            d.material_request = r.get("material_request")
        if r.get("material_request_item") and hasattr(d, "material_request_item"):
            d.material_request_item = r.get("material_request_item")

    receipt.insert(ignore_permissions=True)

    auto_submit = _as_int(_get_first(payload, "submit", default=1), 1)
    if auto_submit and receipt.docstatus == 0:
        receipt.submit()

    return receipt


# -------------------------------------------------------------------
# API: Create transfer to Goods In Transit (and link MR)
# -------------------------------------------------------------------
@frappe.whitelist(methods=["POST"])
def create_material_transfer_add_to_transit(payload=None):
    """
    Create Stock Entry (Material Transfer) from from_warehouse -> to_warehouse (Goods In Transit)
    and link it against a Material Request.

    Expected payload JSON:
    {
      "company": "...",
      "from_warehouse": "...",
      "to_warehouse": "Goods In Transit - ...",
      "custom_receiving_warehouse": "...",   # stored for next step receipt (optional)
      "material_request": "MAT-MR-....",      # OPTIONAL, but recommended
      "remarks": "...",
      "posting_date": "YYYY-MM-DD" (optional)
      "posting_time": "HH:MM:SS" (optional)
      "submit": 1 (default 1)
      "external_ref": "WMS-UUID-..." (optional but recommended for idempotency)
      "items": [
        {"item_code":"108226","qty":6,"material_request_item":"<rowname optional>"}
      ]
    }
    """
    if payload is None:
        payload = _get_payload()
    if not isinstance(payload, dict):
        frappe.throw(_("Invalid payload"))

    company = _cstr(payload.get("company"))
    from_wh = _cstr(payload.get("from_warehouse"))
    to_wh = _cstr(payload.get("to_warehouse"))
    receiving_wh = _cstr(payload.get("custom_receiving_warehouse"))
    remarks = _cstr(payload.get("remarks"))
    material_request = _cstr(payload.get("material_request"))
    external_ref = _cstr(payload.get("external_ref"))

    items = payload.get("items") or []
    if not company:
        frappe.throw(_("company is required"))
    if not from_wh:
        frappe.throw(_("from_warehouse is required"))
    if not to_wh:
        frappe.throw(_("to_warehouse is required"))
    if not items or not isinstance(items, list):
        frappe.throw(_("items is required and must be a list"))

    # Optional idempotency
    existing = _find_existing_by_external_ref(external_ref)
    if existing:
        return {
            "ok": True,
            "version": API_VERSION,
            "message": "Already exists (idempotent)",
            "stock_entry": existing,
        }

    se = frappe.new_doc("Stock Entry")
    se.stock_entry_type = "Material Transfer"
    se.company = company
    se.posting_date = getdate(payload.get("posting_date") or nowdate())
    se.posting_time = payload.get("posting_time") or nowtime()
    se.set_posting_time = 1

    # header warehouses if they exist
    if hasattr(se, "from_warehouse"):
        se.from_warehouse = from_wh
    if hasattr(se, "to_warehouse"):
        se.to_warehouse = to_wh

    se.remarks = _tag_remarks(remarks, external_ref)

    # Store receiving warehouse for later receipt step (if you have this custom field)
    if receiving_wh and hasattr(se, "custom_receiving_warehouse"):
        se.custom_receiving_warehouse = receiving_wh

    # Optional: store external_ref in custom field if exists
    if external_ref and hasattr(se, "custom_external_ref"):
        se.custom_external_ref = external_ref

    # ✅ flag as in-transit transfer (set once, not inside loop)
    if hasattr(se, "add_to_transit"):
        se.add_to_transit = 1

    for row in items:
        if not isinstance(row, dict):
            frappe.throw(_("Each item must be an object"))

        item_code = _cstr(row.get("item_code"))
        qty = float(row.get("qty") or 0)

        if not item_code:
            frappe.throw(_("item_code is required in items"))
        if qty <= 0:
            frappe.throw(_("qty must be > 0 for item {0}").format(item_code))

        d = se.append("items", {})
        d.item_code = item_code
        d.qty = qty

        # Warehouses per line (important!)
        d.s_warehouse = from_wh
        d.t_warehouse = to_wh

        # ✅ MR Linking (optional but recommended)
        if material_request:
            d.material_request = material_request

            mr_item = _cstr(row.get("material_request_item"))
            if not mr_item:
                mr_item = _resolve_mr_item_name(material_request, item_code) or ""
            if mr_item:
                d.material_request_item = mr_item

    se.insert(ignore_permissions=True)

    auto_submit = _as_int(payload.get("submit") if payload.get("submit") is not None else 1, 1)
    if auto_submit and se.docstatus == 0:
        se.submit()

    return {
        "ok": True,
        "version": API_VERSION,
        "stock_entry": se.name,
        "docstatus": se.docstatus,
        "message": "Created transfer to transit",
        "receiving_warehouse": receiving_wh or None,
        "material_request": material_request or None,
        "external_ref": external_ref or None,
    }


# -------------------------------------------------------------------
# API: End Transit -> Create Receipt
# -------------------------------------------------------------------
@frappe.whitelist(methods=["POST", "GET"])
def end_transit_create_receipt(payload=None):
    """
    Endpoint:
      printechs_wms.api.intransit_transfer.end_transit_create_receipt

    Required behavior to stop duplicates:
      1) Must return created/existing receipt Stock Entry name in message.name
      2) Must be idempotent (return existing receipt if already created)

    Required input:
      - in_transit_stock_entry (or accepted aliases)
      - receiving_warehouse OR custom_receiving_warehouse (or fallback from Stock Entry)

    Accepted in-transit SE keys:
      - in_transit_stock_entry
      - in_transit_se
      - stock_entry
      - stock_entry_no
      - transfer_in_stock_entry

    Accepted receiving warehouse keys:
      - receiving_warehouse
      - custom_receiving_warehouse
      - to_warehouse
      - target_warehouse
    """
    p = payload if isinstance(payload, dict) else None
    if p is None:
        p = _get_payload()
    if isinstance(p, str):
        p = frappe.parse_json(p)

    in_transit_se = _cstr(
        _get_first(
            p,
            "in_transit_stock_entry",
            "in_transit_se",
            "stock_entry",
            "stock_entry_no",
            "transfer_in_stock_entry",
        )
    )

    if not in_transit_se:
        frappe.throw(_("in_transit_stock_entry is required"))

    receiving_warehouse = _cstr(
        _get_first(
            p,
            "receiving_warehouse",
            "custom_receiving_warehouse",
            "to_warehouse",
            "target_warehouse",
        )
    )

    # Fallback: read from in-transit Stock Entry custom field if present
    if not receiving_warehouse:
        se_doc = frappe.get_doc("Stock Entry", in_transit_se)
        receiving_warehouse = _cstr(getattr(se_doc, "custom_receiving_warehouse", ""))

    if not receiving_warehouse:
        frappe.throw(_("receiving_warehouse is required (send receiving_warehouse or custom_receiving_warehouse)"))

    # IDEMPOTENCY
    existing = _find_existing_receipt(in_transit_se)
    if existing:
        return {
            "ok": True,
            "api_version": API_VERSION,
            "status": "exists",
            "message": {"name": existing},  # ✅ Desktop reads this
            "data": {"name": existing},
            "receipt_stock_entry": existing,
        }

    # Create new receipt
    receipt_doc = _create_receipt_from_intransit(in_transit_se, receiving_warehouse, p)

    return {
        "ok": True,
        "api_version": API_VERSION,
        "status": "created",
        "message": {"name": receipt_doc.name},  # ✅ Desktop reads this
        "data": {"name": receipt_doc.name},
        "stock_entry": receipt_doc.name,
        "receipt_stock_entry": receipt_doc.name,
    }