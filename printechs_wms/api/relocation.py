# -*- coding: utf-8 -*-
# apps/printechs_wms/printechs_wms/api/relocation.py

from __future__ import annotations

import json
from uuid import uuid4

import frappe
from frappe import _
from frappe.utils import nowdate, now_datetime, cstr, flt


API_VERSION = "relocation_v7_force_company_for_ledger_balance"

SESSION_DT = "WMS Relocation Session"
LINE_DT = "WMS Relocation Line"

LEDGER_DT = "WMS Stock Ledger Entry"
BALANCE_DT = "WMS Stock Balance"


# ============================================================
# COMMON HELPERS
# ============================================================

def _get_meta_safe(dt: str):
    try:
        return frappe.get_meta(dt, ignore_permissions=True)
    except TypeError:
        return frappe.get_meta(dt)


def _pick_existing_field(dt: str, candidates: list[str]) -> str | None:
    meta = _get_meta_safe(dt)
    for f in candidates:
        if meta.has_field(f):
            return f
    return None


def _pick_first_existing(meta, candidates: list[str]) -> str | None:
    for f in candidates:
        if meta.has_field(f):
            return f
    return None


def _set_if_exists(doc, meta, fieldname: str, value):
    if value is None:
        return
    if meta.has_field(fieldname):
        doc.set(fieldname, value)


def _parse_json_if_str(val, default):
    if val is None:
        return default
    if isinstance(val, str):
        try:
            return json.loads(val)
        except Exception:
            return default
    return val


def _get_payload(payload=None) -> dict:
    if payload is not None:
        data = payload if isinstance(payload, dict) else {}
    else:
        j = frappe.request.get_json(silent=True)
        data = j if isinstance(j, dict) else dict(frappe.form_dict or {})

    if isinstance(data, dict) and isinstance(data.get("payload"), dict):
        data = data["payload"]

    return data if isinstance(data, dict) else {}


def _date_from_posting_datetime(v: str | None) -> str | None:
    if not v:
        return None
    s = cstr(v).strip()
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return s[:10]
    return None


def _resolve_warehouse(name: str | None, code: str | None) -> str | None:
    name = cstr(name).strip() if name else None
    code = cstr(code).strip() if code else None

    if name and frappe.db.exists("Warehouse", name):
        return name

    if code:
        wh = frappe.db.get_value("Warehouse", {"code": code}, "name")
        if wh:
            return wh

    return None


def _get_default_company() -> str | None:
    # 1) Global default
    try:
        d = frappe.db.get_single_value("Global Defaults", "default_company")
        if d:
            return d
    except Exception:
        pass

    # 2) System Settings / Defaults table (fallback)
    try:
        d = frappe.db.get_default("company")
        if d:
            return d
    except Exception:
        pass

    # 3) First Company
    return frappe.db.get_value("Company", {}, "name")


def _resolve_company_from_session_or_payload(session_doc, payload_company: str | None) -> str:
    """
    Ledger & Balance require company mandatory.
    Resolve using:
      payload -> session field -> default company
    """
    if payload_company:
        return payload_company

    # try session_doc.company if exists
    c = session_doc.get("company")
    if c:
        return c

    # fallback default company
    d = _get_default_company()
    if d:
        return d

    frappe.throw(_("Company is required (send 'company' in payload and set default company in Global Defaults)"))


def _map_status_to_allowed(doctype: str, incoming: str | None) -> str:
    s = (incoming or "").strip()
    s_up = s.upper()
    mapping = {
        "DRAFT": "Draft",
        "NEW": "Draft",
        "OPEN": "Draft",
        "IN_PROGRESS": "Draft",
        "INPROGRESS": "Draft",
        "SUBMITTED": "Submitted",
        "CONFIRMED": "Submitted",
        "APPROVED": "Submitted",
        "COMPLETED": "Posted",
        "DONE": "Posted",
        "CLOSED": "Posted",
        "POSTED": "Posted",
        "CANCELLED": "Cancelled",
        "CANCELED": "Cancelled",
        "VOID": "Cancelled",
    }
    return mapping.get(s_up, "Draft")


def _pick_allowed_event_type(preferred: str) -> str:
    meta = _get_meta_safe(LEDGER_DT)
    if not meta.has_field("event_type"):
        return preferred

    df = meta.get_field("event_type")
    options = []
    if df and df.options:
        options = [o.strip() for o in cstr(df.options).split("\n") if o.strip()]

    if not options:
        return preferred

    if preferred in options:
        return preferred

    for cand in ["Relocation", "Transfer", "Putaway", "Receive", "Adjustment", "Move"]:
        if cand in options:
            return cand

    return options[0]


# ============================================================
# BALANCE + LEDGER (EXACT FIELDNAMES)
# ============================================================

def _get_balance_qty(company: str, warehouse: str, item_code: str, location: str, carton: str | None) -> float:
    filters = {"company": company, "warehouse": warehouse, "item_code": item_code, "location": location}
    if carton:
        filters["carton"] = carton

    name = frappe.db.get_value(BALANCE_DT, filters, "name")
    if not name:
        return 0.0
    return flt(frappe.db.get_value(BALANCE_DT, name, "qty"))


def update_wms_balance(company: str, warehouse: str, item_code: str, location: str, carton: str | None, qty_delta: float) -> float:
    if not company:
        frappe.throw(_("WMS Stock Balance: company is required"))
    if not warehouse:
        frappe.throw(_("WMS Stock Balance: warehouse is required"))
    if not item_code:
        frappe.throw(_("WMS Stock Balance: item_code is required"))
    if not location:
        frappe.throw(_("WMS Stock Balance: location is required"))

    filters = {"company": company, "warehouse": warehouse, "item_code": item_code, "location": location}
    if carton:
        filters["carton"] = carton

    name = frappe.db.get_value(BALANCE_DT, filters, "name")
    now_dt = now_datetime()

    if name:
        bal = frappe.get_doc(BALANCE_DT, name)
        bal.qty = flt(bal.qty) + flt(qty_delta)
        bal.last_txn_datetime = now_dt
        bal.save(ignore_permissions=True)
        return flt(bal.qty)

    bal = frappe.new_doc(BALANCE_DT)
    bal.company = company
    bal.warehouse = warehouse
    bal.item_code = item_code
    bal.location = location
    if carton:
        bal.carton = carton
    bal.qty = flt(qty_delta)
    bal.last_txn_datetime = now_dt
    bal.insert(ignore_permissions=True)
    return flt(bal.qty)


def _insert_ledger(company: str, item_code: str, location: str, carton: str | None,
                   qty_change: float, qty_after: float | None,
                   event_type: str, voucher_doctype: str, voucher_name: str,
                   wms_txn_id: str, remarks: str | None) -> str | None:

    if not company:
        frappe.throw(_("WMS Stock Ledger Entry: company is required"))
    if not item_code:
        frappe.throw(_("WMS Stock Ledger Entry: item_code is required"))
    if not location:
        frappe.throw(_("WMS Stock Ledger Entry: location is required"))
    if qty_change is None:
        frappe.throw(_("WMS Stock Ledger Entry: qty_change is required"))
    if not wms_txn_id:
        frappe.throw(_("WMS Stock Ledger Entry: wms_txn_id is required"))

    if frappe.db.exists(LEDGER_DT, {"wms_txn_id": wms_txn_id}):
        return None

    led = frappe.new_doc(LEDGER_DT)
    led.posting_datetime = now_datetime()
    led.company = company
    led.item_code = item_code
    led.location = location
    led.qty_change = flt(qty_change)
    led.event_type = _pick_allowed_event_type(event_type)

    if carton:
        led.carton = carton
    if qty_after is not None:
        led.qty_after = flt(qty_after)

    led.voucher_doctype = voucher_doctype
    led.voucher_name = voucher_name
    led.wms_txn_id = wms_txn_id
    if remarks:
        led.remarks = remarks

    led.insert(ignore_permissions=True)
    return led.name


def post_relocation_to_wms_stock(session_doc, payload_company: str | None = None):
    child_fieldname = _pick_existing_field(session_doc.doctype, [
        "lines", "relocation_lines", "items", "relocation_items", "details"
    ])
    if not child_fieldname:
        return

    meta_l = _get_meta_safe(LINE_DT)
    fb_field = _pick_first_existing(meta_l, ["from_bin_location", "from_bin"])
    tb_field = _pick_first_existing(meta_l, ["to_bin_location", "to_bin"])
    fc_field = _pick_first_existing(meta_l, ["from_carton_id", "from_carton"])
    tc_field = _pick_first_existing(meta_l, ["to_carton_id", "to_carton"])

    # ✅ Resolve company reliably
    company = _resolve_company_from_session_or_payload(session_doc, payload_company)

    # If session has company field, force-write it once (so future retries work)
    if session_doc.get("company") != company and _get_meta_safe(session_doc.doctype).has_field("company"):
        session_doc.db_set("company", company, update_modified=False)

    voucher_doctype = session_doc.doctype
    voucher_name = session_doc.name
    remarks = session_doc.get("remarks")
    session_uuid = session_doc.get("session_uuid") or session_doc.name

    for row in session_doc.get(child_fieldname) or []:
        item_code = row.get("item_code")
        qty = flt(row.get("qty"))

        from_wh = row.get("from_warehouse") or session_doc.get("from_warehouse") or session_doc.get("warehouse")
        to_wh = row.get("to_warehouse") or session_doc.get("to_warehouse") or session_doc.get("warehouse")

        from_loc = row.get(fb_field) if fb_field else None
        to_loc = row.get(tb_field) if tb_field else None

        from_carton = row.get(fc_field) if fc_field else None
        to_carton = row.get(tc_field) if tc_field else None

        line_uuid = row.get("line_uuid") or row.get("uuid") or row.name

        if not from_loc or not to_loc:
            frappe.throw(_("Relocation Line {0}: From/To Bin Location is required").format(line_uuid))

        # OUT
        out_txn = f"{session_uuid}:{line_uuid}:OUT"
        out_before = _get_balance_qty(company, from_wh, item_code, from_loc, from_carton)
        out_after = out_before - qty

        _insert_ledger(
            company=company,
            item_code=item_code,
            location=from_loc,
            carton=from_carton,
            qty_change=-qty,
            qty_after=out_after,
            event_type="RELOCATION_OUT",
            voucher_doctype=voucher_doctype,
            voucher_name=voucher_name,
            wms_txn_id=out_txn,
            remarks=remarks,
        )
        update_wms_balance(company, from_wh, item_code, from_loc, from_carton, -qty)

        # IN
        in_txn = f"{session_uuid}:{line_uuid}:IN"
        in_before = _get_balance_qty(company, to_wh, item_code, to_loc, to_carton)
        in_after = in_before + qty

        _insert_ledger(
            company=company,
            item_code=item_code,
            location=to_loc,
            carton=to_carton,
            qty_change=qty,
            qty_after=in_after,
            event_type="RELOCATION_IN",
            voucher_doctype=voucher_doctype,
            voucher_name=voucher_name,
            wms_txn_id=in_txn,
            remarks=remarks,
        )
        update_wms_balance(company, to_wh, item_code, to_loc, to_carton, qty)


# ============================================================
# MAIN API
# ============================================================

@frappe.whitelist(methods=["POST"])
def upsert_relocation_session(payload=None):
    data = _get_payload(payload)

    external_session_id = (
        data.get("external_session_id")
        or data.get("external_id")
        or data.get("session_id")
        or data.get("session_ref")
        or data.get("external_ref")
    )

    payload_company = data.get("company")

    remarks = data.get("remarks")
    mode = data.get("mode")
    policy = data.get("policy")

    posting_date = data.get("posting_date") or _date_from_posting_datetime(data.get("posting_datetime")) or nowdate()
    posting_datetime = data.get("posting_datetime")

    session_uuid = data.get("session_uuid") or data.get("uuid") or data.get("session_guid") or str(uuid4())

    wh_single = (
        data.get("warehouse")
        or data.get("warehouse_name")
        or data.get("default_warehouse")
        or data.get("parent_warehouse")
        or data.get("from_warehouse")
        or data.get("to_warehouse")
    )
    wh_single_resolved = _resolve_warehouse(wh_single, data.get("warehouse_code")) or wh_single

    from_wh_resolved = _resolve_warehouse(data.get("from_warehouse") or wh_single_resolved, data.get("from_warehouse_code")) or (data.get("from_warehouse") or wh_single_resolved)
    to_wh_resolved = _resolve_warehouse(data.get("to_warehouse") or wh_single_resolved, data.get("to_warehouse_code")) or (data.get("to_warehouse") or wh_single_resolved)

    id_field = _pick_existing_field(SESSION_DT, [
        "external_session_id", "external_id", "external_ref", "session_ref", "device_session_id"
    ])

    existing_name = None
    if id_field and external_session_id:
        existing_name = frappe.db.get_value(SESSION_DT, {id_field: external_session_id}, "name")

    if existing_name:
        doc = frappe.get_doc(SESSION_DT, existing_name)
        is_new = False
    else:
        doc = frappe.new_doc(SESSION_DT)
        is_new = True
        if id_field and external_session_id:
            doc.set(id_field, external_session_id)

    meta_s = _get_meta_safe(SESSION_DT)

    if meta_s.has_field("session_uuid"):
        doc.session_uuid = session_uuid
    if meta_s.has_field("posting_date"):
        doc.posting_date = posting_date
    if meta_s.has_field("warehouse"):
        if not wh_single_resolved:
            frappe.throw(_("warehouse is required"))
        doc.warehouse = wh_single_resolved

    status_in = data.get("status")
    status_mapped = _map_status_to_allowed(SESSION_DT, status_in)
    _set_if_exists(doc, meta_s, "status", status_mapped)
    _set_if_exists(doc, meta_s, "external_status", status_in)

    # ✅ set company if the field exists in Session
    if meta_s.has_field("company") and payload_company:
        doc.company = payload_company

    _set_if_exists(doc, meta_s, "mode", mode)
    _set_if_exists(doc, meta_s, "policy", policy)
    _set_if_exists(doc, meta_s, "remarks", remarks)
    _set_if_exists(doc, meta_s, "posting_datetime", posting_datetime)
    _set_if_exists(doc, meta_s, "from_warehouse", from_wh_resolved)
    _set_if_exists(doc, meta_s, "to_warehouse", to_wh_resolved)
    _set_if_exists(doc, meta_s, "last_sync_on", now_datetime())

    lines = _parse_json_if_str(data.get("lines") or data.get("items"), []) or []
    if not isinstance(lines, list):
        lines = []

    child_fieldname = _pick_existing_field(SESSION_DT, [
        "lines", "relocation_lines", "items", "relocation_items", "details"
    ])
    if lines and not child_fieldname:
        frappe.throw(_("Cannot find child table field in {0} (expected 'lines')").format(SESSION_DT))

    meta_l = _get_meta_safe(LINE_DT)
    fb_field = _pick_first_existing(meta_l, ["from_bin_location", "from_bin"])
    tb_field = _pick_first_existing(meta_l, ["to_bin_location", "to_bin"])
    fc_field = _pick_first_existing(meta_l, ["from_carton_id", "from_carton"])
    tc_field = _pick_first_existing(meta_l, ["to_carton_id", "to_carton"])

    if child_fieldname and lines:
        doc.set(child_fieldname, [])
        for i, row in enumerate(lines, start=1):
            if not isinstance(row, dict):
                continue

            child = doc.append(child_fieldname, {})
            _set_if_exists(child, meta_l, "line_uuid", row.get("line_uuid") or row.get("uuid"))
            _set_if_exists(child, meta_l, "item_code", row.get("item_code"))
            _set_if_exists(child, meta_l, "qty", row.get("qty"))

            _set_if_exists(child, meta_l, "from_warehouse", row.get("from_warehouse") or from_wh_resolved)
            _set_if_exists(child, meta_l, "to_warehouse", row.get("to_warehouse") or to_wh_resolved)

            from_bin_val = row.get("from_bin") or row.get("from_bin_location")
            to_bin_val = row.get("to_bin") or row.get("to_bin_location")
            from_carton_val = row.get("from_carton") or row.get("from_carton_id")
            to_carton_val = row.get("to_carton") or row.get("to_carton_id")

            if fb_field:
                child.set(fb_field, from_bin_val)
            if tb_field:
                child.set(tb_field, to_bin_val)
            if fc_field:
                child.set(fc_field, from_carton_val)
            if tc_field:
                child.set(tc_field, to_carton_val)

    # SAVE FIRST
    doc.flags.ignore_permissions = True
    doc.save()

    # POST AFTER SAVE
    if getattr(doc, "status", None) == "Posted":
        post_relocation_to_wms_stock(doc, payload_company=payload_company)

    return {
        "ok": True,
        "version": API_VERSION,
        "name": doc.name,
        "is_new": is_new,
        "status_in": status_in,
        "status_saved": status_mapped,
        "external_session_id": external_session_id,
        "session_uuid": doc.get("session_uuid"),
        "posting_date": doc.get("posting_date"),
        "company_used_for_posting": payload_company or doc.get("company") or _get_default_company(),
    }