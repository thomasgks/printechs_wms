# -*- coding: utf-8 -*-
"""
Desktop -> ERPNext offline sync (push_wms_snapshot).

Payload contract: one batch per event_uuid with stock_transactions, carton_stock, cartons.
- stock_transactions[]: id, transaction_date, transaction_type, reference_doc_type, reference_doc,
  item_code, bin_location, target_bin, carton_id, qty_change, qty_after, notes
- carton_stock[]: carton_id, item_code, bin_location, qty, reserved_qty, last_moved_on
- cartons[]: carton_id, asn_no, status, current_bin_id, remarks

ERPNext mapping:
- WMS Stock Ledger Entry: transaction_date->posting_datetime, target_bin|bin_location->location,
  carton_id->carton (Link), id->wms_txn_id (DESKTOP-STX-<id>), notes->remarks
- WMS Stock Balance: item_code->item_code, bin_location->location, last_moved_on->last_txn_datetime
- WMS Carton: carton_id, asn_no->source_ref, current_bin_id->current_location, status
"""
from __future__ import annotations

import json
import frappe
from frappe.utils import now_datetime, get_datetime

LEDGER_DOCTYPE = "WMS Stock Ledger Entry"
BALANCE_DOCTYPE = "WMS Stock Balance"
CARTON_DOCTYPE = "WMS Carton"
EVENTLOG_DOCTYPE = "WMS Integration Event Log"


# -----------------------------
# Helpers
# -----------------------------

def _dt(v):
    if not v:
        return now_datetime()
    try:
        return get_datetime(v)
    except Exception:
        return now_datetime()


def _as_json(payload) -> str:
    try:
        return json.dumps(payload, ensure_ascii=False, default=str)
    except Exception:
        return str(payload)


def _get_payload():
    payload = frappe.local.form_dict or {}
    if isinstance(payload, dict) and "data" in payload and isinstance(payload["data"], dict):
        payload = payload["data"]
    if isinstance(payload, dict) and "payload" in payload and isinstance(payload["payload"], dict):
        payload = payload["payload"]
    return payload


def _map_carton_status(v: str) -> str:
    v = (v or "").strip().lower()
    if v in ("open", "opened", "new"):
        return "Open"
    if v in ("closed", "stored", "putaway", "putaway_done", "completed", "saved"):
        return "Closed"
    if v in ("consumed", "picked", "issued", "delivered", "shipped"):
        return "Consumed"
    if v in ("damaged", "damage"):
        return "Damaged"
    return "Open"


_EVENT_TYPE_MAP = {
    "PUTAWAY": "Putaway",
    "RECEIVE": "Receive", "RECEIVING": "Receive", "GRN": "Receive",
    "TRANSFER": "Transfer", "TRANSFERIN": "Transfer", "TRANSFER_IN": "Transfer",
    "TRANSFEROUT": "Transfer", "TRANSFER_OUT": "Transfer",
    "MOVE": "Transfer",
    "PICK": "Pick", "PICKING": "Pick",
    "CYCLECOUNT": "CycleCount", "CYCLE_COUNT": "CycleCount",
    "ADJUST": "Adjust", "ADJUSTMENT": "Adjust",
}


def _map_event_type(v: str) -> str:
    if not v or not str(v).strip():
        return "Adjust"
    key = (str(v).strip()).upper().replace("-", "_").replace(" ", "")
    return _EVENT_TYPE_MAP.get(key, "Adjust")


def _get_bin_from_row(row):
    """Return (to_bin, from_bin) from row. Tries explicit keys first, then any key that looks like bin/location."""
    to_keys = ("target_bin", "to_bin", "to_location", "destination_bin", "destination", "bin_to")
    from_keys = ("bin_location", "from_bin", "from_location", "source_bin", "source", "bin_from", "bin", "location")
    to_val = ""
    from_val = ""
    for k in to_keys:
        v = row.get(k)
        if v is not None and str(v).strip():
            to_val = str(v).strip()
            break
    for k in from_keys:
        v = row.get(k)
        if v is not None and str(v).strip():
            from_val = str(v).strip()
            break
    # Fallback: use any row value whose key suggests bin/location (e.g. ToBin, bin_id, putaway_location)
    if not to_val and not from_val and isinstance(row, dict):
        dest_like = ("to", "target", "destination", "putaway", "final")
        for k, v in row.items():
            if v is None or not str(v).strip():
                continue
            k_lower = (k or "").lower()
            if not any(x in k_lower for x in ("bin", "location", "loc")):
                continue
            s = str(v).strip()
            if any(x in k_lower for x in dest_like):
                to_val = s
                break
            from_val = s  # last non-destination bin-like key wins for from_val
    return (to_val, from_val)


def _log_event(event_uuid, event_type, source_system, status, payload, processed=None, error=None):
    """Never raises. If Event Log table missing or any DB error, log and return None."""
    try:
        if not frappe.db.exists("DocType", EVENTLOG_DOCTYPE):
            try:
                frappe.log_error(
                    title="WMS Integration Event Log missing",
                    message=f"DocType {EVENTLOG_DOCTYPE} does not exist. event_uuid={event_uuid}",
                )
            except Exception:
                pass
            return None
        table_name = f"tab{EVENTLOG_DOCTYPE}"
        if not frappe.db.table_exists(table_name):
            try:
                frappe.log_error(
                    title="WMS Integration Event Log missing",
                    message=f"Table {table_name} does not exist. event_uuid={event_uuid}",
                )
            except Exception:
                pass
            return None
        payload_json = _as_json(payload)
        processed_json = _as_json(processed) if processed is not None else None
        name = frappe.db.get_value(EVENTLOG_DOCTYPE, {"event_uuid": event_uuid}, "name")
        if name:
            values = {
                "event_type": event_type,
                "event_time": now_datetime(),
                "source_system": source_system,
                "status": status,
                "payload_json": payload_json,
            }
            try:
                if processed_json is not None and frappe.db.has_column(table_name, "processed_json"):
                    values["processed_json"] = processed_json
            except Exception:
                pass
            try:
                if error and frappe.db.has_column(table_name, "error"):
                    values["error"] = error
            except Exception:
                pass
            frappe.db.set_value(EVENTLOG_DOCTYPE, name, values, update_modified=True)
            return name
        doc = frappe.new_doc(EVENTLOG_DOCTYPE)
        doc.event_uuid = event_uuid
        doc.event_type = event_type
        doc.event_time = now_datetime()
        doc.source_system = source_system
        doc.status = status
        doc.payload_json = payload_json
        if processed_json is not None and hasattr(doc, "processed_json"):
            doc.processed_json = processed_json
        if error and hasattr(doc, "error"):
            doc.error = error
        doc.insert(ignore_permissions=True)
        return doc.name
    except Exception as e:
        try:
            frappe.log_error(
                title="WMS Integration Event Log error",
                message=f"event_uuid={event_uuid} error={e}",
            )
        except Exception:
            pass
        return None


def upsert_wms_carton(row: dict, company: str) -> str:
    carton_id = (row.get("carton_id") or "").strip()
    if not carton_id:
        frappe.throw("cartons[].carton_id is required")
    values = {
        "company": company,
        "status": _map_carton_status(row.get("status")),
        "source_type": row.get("source_type") or "ASN",
        "source_ref": row.get("asn_no") or row.get("source_ref"),
        "current_location": row.get("current_bin_id") or row.get("current_location"),
        "remarks": row.get("remarks"),
    }
    name = frappe.db.get_value(CARTON_DOCTYPE, {"carton_id": carton_id}, "name")
    if name:
        frappe.db.set_value(CARTON_DOCTYPE, name, values, update_modified=True)
        return name
    doc = frappe.new_doc(CARTON_DOCTYPE)
    doc.carton_id = carton_id
    for k, v in values.items():
        if hasattr(doc, k):
            setattr(doc, k, v)
    doc.insert(ignore_permissions=True)
    return doc.name


def upsert_wms_stock_balance(row: dict, company: str) -> str:
    item_code = (row.get("item_code") or row.get("item") or "").strip()
    location = (
        row.get("bin_location") or row.get("location") or row.get("target_bin")
        or row.get("to_bin") or row.get("from_bin") or ""
    ).strip()
    carton = (row.get("carton_id") or row.get("carton") or "") or ""
    if not item_code or not location:
        frappe.throw("carton_stock[] requires item_code/item and bin_location/location/target_bin/to_bin/from_bin")
    qty = float(row.get("qty") or row.get("qty_after") or 0)
    reserved_qty = float(row.get("reserved_qty") or 0)
    last_txn = _dt(row.get("last_moved_on") or row.get("last_txn_datetime") or row.get("transaction_date"))
    name = frappe.db.get_value(
        BALANCE_DOCTYPE,
        {"company": company, "item_code": item_code, "location": location, "carton": carton},
        "name",
    )
    if name:
        frappe.db.set_value(
            BALANCE_DOCTYPE,
            name,
            {"qty": qty, "reserved_qty": reserved_qty, "last_txn_datetime": last_txn},
            update_modified=True,
        )
        return name
    doc = frappe.new_doc(BALANCE_DOCTYPE)
    doc.company = company
    doc.item_code = item_code
    doc.location = location
    doc.carton = carton
    doc.qty = qty
    doc.reserved_qty = reserved_qty
    doc.last_txn_datetime = last_txn
    doc.insert(ignore_permissions=True)
    return doc.name


def upsert_wms_stock_ledger(row: dict, company: str, source_system: str = "WMS") -> str:
    raw_id = row.get("id")
    if raw_id is None or str(raw_id).strip() == "":
        frappe.throw("stock_transactions[].id is required")
    wms_txn_id = f"DESKTOP-STX-{raw_id}"
    existing = frappe.db.get_value(LEDGER_DOCTYPE, {"wms_txn_id": wms_txn_id}, "name")
    if existing:
        return existing
    to_bin, from_bin_val = _get_bin_from_row(row)
    location = to_bin or from_bin_val
    if not location:
        frappe.throw(
            "stock_transactions[]: provide at least one of target_bin, to_bin, bin_location, from_bin, bin, location, to_location, from_location, destination_bin, source_bin"
        )
    doc = frappe.new_doc(LEDGER_DOCTYPE)
    doc.posting_datetime = _dt(row.get("transaction_date"))
    doc.company = company
    doc.item_code = row.get("item_code")
    doc.location = location
    doc.carton = row.get("carton_id")
    doc.qty_change = row.get("qty_change") or row.get("qty_delta") or 0
    doc.qty_after = row.get("qty_after")
    doc.event_type = _map_event_type(row.get("transaction_type") or "Adjust")
    doc.voucher_doctype = row.get("reference_doc_type")
    doc.voucher_name = row.get("reference_doc")
    doc.wms_txn_id = wms_txn_id
    doc.remarks = row.get("notes")
    if hasattr(doc, "from_bin") and from_bin_val:
        doc.from_bin = from_bin_val
    if hasattr(doc, "to_bin") and to_bin:
        doc.to_bin = to_bin
    if hasattr(doc, "source_row_id"):
        doc.source_row_id = str(raw_id)
    if hasattr(doc, "source_doctype"):
        doc.source_doctype = row.get("reference_doc_type")
    if hasattr(doc, "source_docname"):
        doc.source_docname = row.get("reference_doc")
    if hasattr(doc, "source_system"):
        doc.source_system = source_system or "Desktop"
    doc.insert(ignore_permissions=True)
    return doc.name


@frappe.whitelist(methods=["POST"])
def push_wms_snapshot():
    payload = _get_payload()
    event_uuid = payload.get("event_uuid")
    company = payload.get("company")
    source_system = payload.get("source_system") or "WMS"
    if not event_uuid:
        frappe.throw("event_uuid is required")
    if not company:
        frappe.throw("company is required")

    stock_txns = payload.get("stock_transactions") or []
    if stock_txns and isinstance(stock_txns[0], dict):
        event_type = _map_event_type(stock_txns[0].get("transaction_type"))
    else:
        event_type = "Adjust"

    if frappe.db.table_exists(f"tab{EVENTLOG_DOCTYPE}"):
        already_success = frappe.db.exists(EVENTLOG_DOCTYPE, {"event_uuid": event_uuid, "status": "Success"})
        if already_success:
            return {
                "ok": True,
                "event_uuid": event_uuid,
                "event_type": event_type,
                "processed": {"ledger": 0, "carton_stock": 0, "cartons": 0},
                "errors": [],
            }

    processed = {"ledger": 0, "carton_stock": 0, "cartons": 0}
    errors = []
    _log_event(event_uuid, event_type, source_system, "Processed", payload, processed=processed, error=None)

    try:
        for row in (payload.get("cartons") or []):
            try:
                upsert_wms_carton(row, company)
                processed["cartons"] += 1
            except Exception as e:
                errors.append("cartons: " + (str(e) or frappe.get_traceback()))

        for row in (payload.get("stock_transactions") or []):
            try:
                carton_id = (row.get("carton_id") or "").strip()
                if carton_id and not frappe.db.exists(CARTON_DOCTYPE, {"carton_id": carton_id}):
                    to_b, from_b = _get_bin_from_row(row)
                    upsert_wms_carton(
                        {
                            "carton_id": carton_id,
                            "status": "Open",
                            "current_bin_id": to_b or from_b,
                            "source_type": row.get("source_type") or "ASN",
                            "source_ref": row.get("source_ref") or row.get("reference_doc"),
                        },
                        company,
                    )
                    processed["cartons"] += 1
                upsert_wms_stock_ledger(row, company, source_system=source_system)
                processed["ledger"] += 1
            except Exception as e:
                errors.append("ledger: " + (str(e) or frappe.get_traceback()))

        for row in (payload.get("carton_stock") or []):
            try:
                upsert_wms_stock_balance(row, company)
                processed["carton_stock"] += 1
            except Exception as e:
                errors.append("carton_stock: " + (str(e) or frappe.get_traceback()))

        if not (payload.get("carton_stock") or []) and (payload.get("stock_transactions") or []):
            for row in payload.get("stock_transactions") or []:
                try:
                    to_b, from_b = _get_bin_from_row(row)
                    loc = to_b or from_b
                    derived = {
                        "item_code": row.get("item_code"),
                        "bin_location": loc,
                        "location": loc,
                        "carton_id": row.get("carton_id"),
                        "qty_after": row.get("qty_after"),
                        "transaction_date": row.get("transaction_date"),
                    }
                    if derived.get("item_code") and derived.get("location"):
                        upsert_wms_stock_balance(derived, company)
                        processed["carton_stock"] += 1
                except Exception as e:
                    errors.append("carton_stock: " + (str(e) or frappe.get_traceback()))

        status = "Success" if not errors else "Failed"
        _log_event(
            event_uuid,
            event_type,
            source_system,
            status,
            payload,
            processed=processed,
            error="\n".join(errors) if errors else None,
        )
        return {
            "ok": len(errors) == 0,
            "event_uuid": event_uuid,
            "event_type": event_type,
            "processed": processed,
            "errors": errors,
        }
    except Exception as e:
        _log_event(
            event_uuid,
            event_type,
            source_system,
            "Failed",
            payload,
            processed=processed,
            error=str(e),
        )
        return {
            "ok": False,
            "event_uuid": event_uuid,
            "event_type": event_type,
            "processed": processed,
            "errors": [str(e)],
        }
