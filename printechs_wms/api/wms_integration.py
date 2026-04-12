# apps/printechs_wms/printechs_wms/api/wms_integration.py

import json
import frappe
from frappe import _

# -------------------------
# Helpers
# -------------------------

def _get_settings():
    return frappe.get_single("WMS Integration Settings")


def _get_payload():
    """Accept JSON from WMS (raw) or form_dict; return dict"""
    raw = frappe.request.get_data(as_text=True) if frappe.request else None
    if raw:
        try:
            return json.loads(raw)
        except Exception:
            pass
    # fallback
    if isinstance(frappe.local.form_dict, dict) and frappe.local.form_dict:
        return dict(frappe.local.form_dict)
    return {}


def _ensure_event_uuid(payload: dict) -> str:
    event_uuid = payload.get("event_uuid")
    if not event_uuid:
        frappe.throw(_("event_uuid is required"))
    return event_uuid


def _is_event_processed(event_uuid: str):
    """Return existing processed reference if already processed."""
    row = frappe.db.get_value(
        "WMS Integration Event Log",
        {"event_uuid": event_uuid, "status": "Processed"},
        ["reference_doctype", "reference_name"],
        as_dict=True
    )
    return row


def _create_event_log(payload: dict):
    """Insert event row as Received. If duplicate key, treat as already received."""
    doc = frappe.get_doc({
        "doctype": "WMS Integration Event Log",
        "event_uuid": payload.get("event_uuid"),
        "event_type": payload.get("event_type"),
        "event_time": payload.get("event_time"),
        "status": "Received",
        "payload_json": json.dumps(payload, ensure_ascii=False),
    })
    doc.insert(ignore_permissions=True)
    return doc.name


def _mark_processed(event_uuid: str, ref_doctype: str, ref_name: str):
    frappe.db.set_value(
        "WMS Integration Event Log",
        {"event_uuid": event_uuid},
        {
            "status": "Processed",
            "reference_doctype": ref_doctype,
            "reference_name": ref_name
        }
    )


def _mark_failed(event_uuid: str, error: str):
    frappe.db.set_value(
        "WMS Integration Event Log",
        {"event_uuid": event_uuid},
        {"status": "Failed", "error": error}
    )


def _upsert_posting_queue(source_type: str, source_id: str, ref_doctype: str, ref_name: str, status: str = "Draft"):
    if not source_id:
        return

    existing = frappe.db.exists("WMS Posting Queue", {"source_type": source_type, "source_id": source_id})
    if existing:
        frappe.db.set_value("WMS Posting Queue", existing, {
            "erp_reference_doctype": ref_doctype,
            "erp_reference_name": ref_name,
            "posting_status": status,
            "last_error": None
        })
        return

    doc = frappe.get_doc({
        "doctype": "WMS Posting Queue",
        "source_type": source_type,
        "source_id": source_id,
        "erp_reference_doctype": ref_doctype,
        "erp_reference_name": ref_name,
        "posting_status": status
    })
    doc.insert(ignore_permissions=True)


# -------------------------
# 1) ASN_RECEIVED -> Purchase Receipt (Draft)
# -------------------------
@frappe.whitelist(methods=["POST"])
def asn_received():
    payload = _get_payload()
    event_uuid = _ensure_event_uuid(payload)

    # Idempotency
    existing = _is_event_processed(event_uuid)
    if existing:
        return {"status": "duplicate", "reference": existing}

    # Create log row as Received (handles first time processing)
    try:
        _create_event_log(payload)
    except Exception:
        # If unique constraint already inserted earlier, continue but keep idempotency safe
        existing2 = _is_event_processed(event_uuid)
        if existing2:
            return {"status": "duplicate", "reference": existing2}

    settings = _get_settings()
    if not settings.enable_auto_create_pr_draft:
        return {"status": "ok", "note": "Auto PR draft creation disabled"}

    try:
        asn_id = payload.get("asn_id")
        supplier = payload.get("supplier_code") or payload.get("supplier")
        if not supplier:
            frappe.throw(_("supplier_code is required"))

        receiving_wh = payload.get("receiving_warehouse") or settings.default_receiving_warehouse
        if not receiving_wh:
            frappe.throw(_("receiving_warehouse (or default_receiving_warehouse in settings) is required"))

        po_no = payload.get("po_no")

        # Build PR Draft
        pr = frappe.new_doc("Purchase Receipt")
        pr.supplier = supplier
        pr.posting_date = frappe.utils.nowdate()
        pr.set_warehouse = receiving_wh

        # Optional reference tracking - if you later create custom fields, map here
        # pr.custom_asn_id = asn_id
        # pr.custom_wms_asn_id = payload.get("wms_asn_id")

        lines = payload.get("line_totals") or []
        if not isinstance(lines, list) or len(lines) == 0:
            frappe.throw(_("line_totals[] is required (use item totals, not carton lines)"))

        for row in lines:
            item_code = row.get("item_code")
            qty = row.get("qty")
            if not item_code or qty is None:
                frappe.throw(_("Each line_totals row must have item_code and qty"))

            pr.append("items", {
                "item_code": item_code,
                "qty": qty,
                "uom": row.get("uom") or "Nos",
                "warehouse": receiving_wh,
                # Link PO if provided
                "purchase_order": po_no
            })

        pr.insert(ignore_permissions=True)  # Draft

        _mark_processed(event_uuid, "Purchase Receipt", pr.name)

        _upsert_posting_queue(
            source_type="ASN",
            source_id=payload.get("wms_asn_id") or asn_id,
            ref_doctype="Purchase Receipt",
            ref_name=pr.name,
            status="Draft"
        )

        frappe.db.commit()
        return {"status": "ok", "purchase_receipt": pr.name, "docstatus": pr.docstatus}

    except Exception:
        frappe.db.rollback()
        _mark_failed(event_uuid, frappe.get_traceback())
        frappe.db.commit()
        frappe.throw(_("ASN_RECEIVED failed. Check WMS Integration Event Log for error."))


# -------------------------
# 2) TRANSFER_COMPLETED -> Stock Entry (Draft)
# -------------------------
@frappe.whitelist(methods=["POST"])
def warehouse_transfer_completed():
    payload = _get_payload()
    event_uuid = _ensure_event_uuid(payload)

    existing = _is_event_processed(event_uuid)
    if existing:
        return {"status": "duplicate", "reference": existing}

    try:
        _create_event_log(payload)
    except Exception:
        existing2 = _is_event_processed(event_uuid)
        if existing2:
            return {"status": "duplicate", "reference": existing2}

    settings = _get_settings()
    if not settings.enable_auto_create_stock_entry_draft:
        return {"status": "ok", "note": "Auto Stock Entry draft creation disabled"}

    try:
        src_wh = payload.get("source_warehouse") or settings.default_source_warehouse
        tgt_wh = payload.get("target_warehouse") or settings.default_target_warehouse
        if not src_wh or not tgt_wh:
            frappe.throw(_("source_warehouse and target_warehouse are required (or set defaults in settings)"))

        lines = payload.get("line_totals") or []
        if not isinstance(lines, list) or len(lines) == 0:
            frappe.throw(_("line_totals[] is required"))

        se = frappe.new_doc("Stock Entry")
        se.stock_entry_type = "Material Transfer"
        se.posting_date = frappe.utils.nowdate()

        for row in lines:
            item_code = row.get("item_code")
            qty = row.get("qty")
            if not item_code or qty is None:
                frappe.throw(_("Each line_totals row must have item_code and qty"))

            se.append("items", {
                "item_code": item_code,
                "qty": qty,
                "uom": row.get("uom") or "Nos",
                "s_warehouse": src_wh,
                "t_warehouse": tgt_wh
            })

        se.insert(ignore_permissions=True)  # Draft

        _mark_processed(event_uuid, "Stock Entry", se.name)

        _upsert_posting_queue(
            source_type="Transfer",
            source_id=payload.get("wms_transfer_id") or payload.get("transfer_order_id"),
            ref_doctype="Stock Entry",
            ref_name=se.name,
            status="Draft"
        )

        frappe.db.commit()
        return {"status": "ok", "stock_entry": se.name, "docstatus": se.docstatus}

    except Exception:
        frappe.db.rollback()
        _mark_failed(event_uuid, frappe.get_traceback())
        frappe.db.commit()
        frappe.throw(_("WAREHOUSE_TRANSFER_COMPLETED failed. Check WMS Integration Event Log for error."))
