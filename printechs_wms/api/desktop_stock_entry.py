# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import frappe
from frappe import _
from frappe.utils import nowdate, nowtime, getdate


API_VERSION = "desktop_stock_entry_v3_no_timestamp_mismatch"


def _cstr(v) -> str:
    try:
        return str(v or "").strip()
    except Exception:
        return ""


def _flt(v) -> float:
    try:
        return float(v or 0)
    except Exception:
        return 0.0


def _as_int(v, default=0) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _parse_json_if_str(val, default):
    if val is None:
        return default
    if isinstance(val, str):
        try:
            return json.loads(val)
        except Exception:
            return default
    return val


def _get_payload() -> dict:
    data = dict(frappe.form_dict or {})
    try:
        j = frappe.request.get_json(silent=True)
        if isinstance(j, dict):
            data.update(j)
    except Exception:
        pass

    p = data.get("payload")
    p = _parse_json_if_str(p, None)
    if isinstance(p, dict):
        return p
    return data


def _tag_remarks(remarks: str, external_ref: str) -> str:
    remarks = _cstr(remarks)
    external_ref = _cstr(external_ref)
    if not external_ref:
        return remarks
    tag = f"[EXT:{external_ref}]"
    if tag in remarks:
        return remarks
    return f"{tag} {remarks}".strip()


def _resolve_warehouse(name: str | None, code: str | None) -> str:
    name = _cstr(name)
    code = _cstr(code)

    if name and frappe.db.exists("Warehouse", name):
        return name

    if code:
        wh = frappe.db.get_value("Warehouse", {"code": code}, "name")
        if wh:
            return wh

    frappe.throw(
        _("Warehouse not found. Provide valid warehouse name or code. name={0}, code={1}")
        .format(name or "-", code or "-")
    )


def _find_existing_by_external_ref(external_ref: str) -> str | None:
    external_ref = _cstr(external_ref)
    if not external_ref:
        return None

    if frappe.db.has_column("Stock Entry", "custom_external_ref"):
        se = frappe.db.get_value("Stock Entry", {"custom_external_ref": external_ref}, "name")
        if se:
            return se

    tag = f"[EXT:{external_ref}]"
    se = frappe.db.get_value("Stock Entry", {"remarks": ["like", f"%{tag}%"]}, "name")
    return se


def _set_if_exists(doc, fieldname: str, value) -> bool:
    try:
        if frappe.get_meta(doc.doctype).has_field(fieldname):
            doc.set(fieldname, value)
            return True
    except Exception:
        pass
    return False


@frappe.whitelist(methods=["POST"])
def create_stock_entry_from_transfer_carton(payload=None):
    """
    Transfer Carton -> Stock Entry (Material Transfer) + Add to Transit
    Fix: custom_receiving_warehouse forced update without timestamp mismatch.
    """
    if payload is None:
        payload = _get_payload()

    payload = _parse_json_if_str(payload, payload)
    if not isinstance(payload, dict):
        frappe.throw(_("payload must be a JSON object"))

    transfer_carton_id = _cstr(payload.get("transfer_carton_id"))
    asn_no = _cstr(payload.get("asn_no"))
    transfer_order = _cstr(payload.get("transfer_order"))

    company = _cstr(payload.get("company"))
    if not company:
        frappe.throw(_("company is required"))

    items = payload.get("items") or []
    if not items or not isinstance(items, list):
        frappe.throw(_("items cannot be empty and must be a list"))

    # Warehouses (name or code)
    from_wh = _resolve_warehouse(payload.get("from_warehouse"), payload.get("from_warehouse_code"))
    to_wh = _resolve_warehouse(payload.get("to_warehouse"), payload.get("to_warehouse_code"))

    receiving_wh = _cstr(payload.get("custom_receiving_warehouse"))
    remarks = _cstr(payload.get("remarks"))
    external_ref = _cstr(payload.get("external_ref"))

    posting_date = payload.get("posting_date") or nowdate()
    posting_time = payload.get("posting_time") or nowtime()
    submit_flag = _as_int(payload.get("submit") if payload.get("submit") is not None else 1, 1)

    # fallback idempotency
    if not external_ref and transfer_carton_id:
        external_ref = f"TC:{transfer_carton_id}"

    # Idempotency
    existing = _find_existing_by_external_ref(external_ref)
    if existing:
        saved_recv = frappe.db.get_value("Stock Entry", existing, "custom_receiving_warehouse") \
            if frappe.db.has_column("Stock Entry", "custom_receiving_warehouse") else None
        return {
            "ok": True,
            "version": API_VERSION,
            "message": "Already exists (idempotent)",
            "stock_entry_no": existing,
            "external_ref": external_ref or None,
            "saved_custom_receiving_warehouse": saved_recv,
        }

    se = frappe.new_doc("Stock Entry")
    se.stock_entry_type = "Material Transfer"
    se.company = company
    se.posting_date = getdate(posting_date)
    se.posting_time = posting_time
    se.set_posting_time = 1

    _set_if_exists(se, "from_warehouse", from_wh)
    _set_if_exists(se, "to_warehouse", to_wh)

    # Add to transit flag
    _set_if_exists(se, "add_to_transit", 1)

    # External ref store
    if external_ref:
        _set_if_exists(se, "custom_external_ref", external_ref)

    # Set receiving warehouse before insert (normal)
    if receiving_wh:
        _set_if_exists(se, "custom_receiving_warehouse", receiving_wh)

    se.remarks = _tag_remarks(remarks, external_ref)

    for row in items:
        row = row or {}
        item_code = _cstr(row.get("item_code"))
        qty = _flt(row.get("qty"))
        uom = _cstr(row.get("uom"))
        source_carton = _cstr(row.get("source_carton"))

        if not item_code:
            frappe.throw(_("Item Code missing in item row"))
        if qty <= 0:
            frappe.throw(_("Invalid quantity for item {0}").format(item_code))

        d = se.append("items", {})
        d.item_code = item_code
        d.qty = qty
        if uom:
            d.uom = uom

        d.s_warehouse = from_wh
        d.t_warehouse = to_wh

        if source_carton and hasattr(d, "source_carton"):
            d.source_carton = source_carton

    se.insert(ignore_permissions=True)

    # ✅ Force write receiving warehouse WITHOUT touching modified timestamp
    # This avoids TimestampMismatchError during submit.
    if receiving_wh and frappe.db.has_column("Stock Entry", "custom_receiving_warehouse"):
        frappe.db.set_value(
            "Stock Entry",
            se.name,
            "custom_receiving_warehouse",
            receiving_wh,
            update_modified=False
        )

    if submit_flag and se.docstatus == 0:
        # No reload needed because we avoided updating modified timestamp
        se.submit()

    saved_recv = frappe.db.get_value("Stock Entry", se.name, "custom_receiving_warehouse") \
        if frappe.db.has_column("Stock Entry", "custom_receiving_warehouse") else None

    return {
        "ok": True,
        "version": API_VERSION,
        "message": "Created transfer carton -> transit Stock Entry",
        "stock_entry_no": se.name,
        "docstatus": se.docstatus,
        "submitted": bool(se.docstatus == 1),
        "external_ref": external_ref or None,
        "resolved": {
            "from_warehouse": from_wh,
            "to_warehouse": to_wh,
        },
        "reference": {
            "transfer_carton_id": transfer_carton_id or None,
            "asn_no": asn_no or None,
            "transfer_order": transfer_order or None,
        },
        "payload_custom_receiving_warehouse": receiving_wh or None,
        "saved_custom_receiving_warehouse": saved_recv,
    }