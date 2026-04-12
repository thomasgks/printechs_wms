from __future__ import annotations

import frappe
from frappe import _

API_VERSION = "asn_receiving_v1"

ASN_DOCTYPE = "WMS ASN"
ASN_ITEM_PARENTFIELD = "items"  # child table fieldname on ASN

# --- CHILD FIELDNAMES (fixed) ---
F_ITEM_CODE = "item_code"
F_CARTON_ID = "carton_id"

# shipped/received will be AUTO-DETECTED, but we keep candidate lists:
RECV_QTY_CANDIDATES = ["recvd_qty", "received_qty", "qty_received", "received", "received_quantity", "recv_qty"]
SHIPPED_QTY_CANDIDATES = ["shipped_qty", "qty_shipped", "shipped", "shipped_quantity"]

# optional per-row statuses (as per your screenshot)
F_ROW_CARTON_STATUS = "carton_status"       # Select: Pending/Received/Partially Received
F_ROW_RECEIVING_STATUS = "receiving_status" # Select: Pending/Received/Partially Received

# --- ASN HEADER FIELD ---
F_ASN_STATUS = "status"


def _as_float(v, default=0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _as_int(v, default=0) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _as_str(v) -> str:
    return "" if v is None else str(v).strip()


def _get_payload() -> dict:
    """
    Always returns dict. Supports JSON raw body and form_dict.
    """
    req = getattr(frappe, "request", None)

    if req and getattr(req, "data", None):
        raw = req.data

        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", errors="ignore")

        if isinstance(raw, dict):
            return raw

        if isinstance(raw, str) and raw.strip():
            try:
                parsed = frappe.parse_json(raw)
                if isinstance(parsed, dict):
                    return parsed
            except Exception:
                pass

    if isinstance(frappe.form_dict, dict) and frappe.form_dict:
        return dict(frappe.form_dict)

    return {}


def _pick_qty(line: dict) -> float:
    """
    Accepts multiple possible qty keys.
    Priority: qty -> received_qty -> recvd_qty -> <whatever else>
    """
    if not isinstance(line, dict):
        return 0.0

    if line.get("qty") is not None:
        return _as_float(line.get("qty"), 0.0)

    if line.get("received_qty") is not None:
        return _as_float(line.get("received_qty"), 0.0)

    if line.get("recvd_qty") is not None:
        return _as_float(line.get("recvd_qty"), 0.0)

    return 0.0


def _resolve_child_field(meta, candidates: list[str], fallback_label_keywords: list[str] | None = None) -> str | None:
    """
    Try exact fieldname candidates first.
    If not found, optionally scan by label keywords.
    """
    # exact candidates
    for f in candidates:
        if meta.has_field(f):
            return f

    # fallback by label keywords
    if fallback_label_keywords:
        keys = [k.lower() for k in fallback_label_keywords]
        for df in meta.fields:
            lbl = (df.label or "").lower()
            if any(k in lbl for k in keys):
                return df.fieldname

    return None


def _get_child_meta(doc) -> frappe.model.meta.Meta:
    """
    Get child table DocType meta from parent doc.
    """
    df = doc.meta.get_field(ASN_ITEM_PARENTFIELD)
    if not df or df.fieldtype != "Table" or not df.options:
        frappe.throw(_("Invalid child table config: {0}").format(ASN_ITEM_PARENTFIELD))
    return frappe.get_meta(df.options)


def _find_row(doc, *, child_name=None, item_code=None, carton_id=None):
    """
    Match priority:
      1) child row by name
      2) match by item_code + carton_id
      3) match by item_code only
    """
    rows = doc.get(ASN_ITEM_PARENTFIELD) or []

    if child_name:
        for r in rows:
            if r.name == child_name:
                return r
        return None

    item_code = _as_str(item_code)
    carton_id = _as_str(carton_id)

    if item_code and carton_id:
        for r in rows:
            if _as_str(r.get(F_ITEM_CODE)) == item_code and _as_str(r.get(F_CARTON_ID)) == carton_id:
                return r
        return None

    if item_code:
        for r in rows:
            if _as_str(r.get(F_ITEM_CODE)) == item_code:
                return r
        return None

    return None


def _row_receiving_status(shipped: float, recvd: float) -> str:
    if recvd <= 0:
        return "Pending"
    if shipped > 0 and recvd + 1e-9 >= shipped:
        return "Received"
    return "Partially Received"


def _recalc_header_status(doc, shipped_field: str, recvd_field: str) -> dict:
    total_shipped = 0.0
    total_recvd = 0.0

    for r in doc.get(ASN_ITEM_PARENTFIELD) or []:
        total_shipped += _as_float(r.get(shipped_field), 0.0)
        total_recvd += _as_float(r.get(recvd_field), 0.0)

    if total_recvd <= 0:
        new_status = "Pending"
    elif total_shipped > 0 and total_recvd + 1e-9 >= total_shipped:
        new_status = "Received"
    else:
        new_status = "Partially Received"

    if doc.meta.has_field(F_ASN_STATUS):
        doc.set(F_ASN_STATUS, new_status)

    return {"total_shipped": total_shipped, "total_recvd": total_recvd, "new_status": new_status}


def _db_set(doctype: str, name: str, values: dict):
    """
    Direct DB update. Works even when parent is Submitted.
    """
    for fieldname, val in values.items():
        frappe.db.set_value(doctype, name, fieldname, val, update_modified=False)


@frappe.whitelist(methods=["POST", "PUT"])
def update_asn_received_qty(**kwargs):
    """
    URL:
      /api/method/printechs_wms.api.asn_receiving.update_asn_received_qty
    """
    payload = _get_payload()
    if kwargs and isinstance(kwargs, dict):
        payload.update(kwargs)

    asn_no = _as_str(payload.get("asn_no") or payload.get("asn") or payload.get("name"))
    mode = _as_str(payload.get("mode") or "increment").lower()
    update_status = _as_int(payload.get("update_status"), 0)
    allow_submitted = _as_int(payload.get("allow_submitted"), 1)

    lines = payload.get("lines") or payload.get("items") or []
    if isinstance(lines, str):
        try:
            lines = frappe.parse_json(lines)
        except Exception:
            lines = []

    if not asn_no:
        frappe.throw(_("asn_no is required"))
    if mode not in ("increment", "set"):
        frappe.throw(_("mode must be 'increment' or 'set'"))
    if not isinstance(lines, list) or not lines:
        frappe.throw(_("lines must be a non-empty list"))
    if not frappe.db.exists(ASN_DOCTYPE, asn_no):
        frappe.throw(_("{0} not found: {1}").format(ASN_DOCTYPE, asn_no))

    doc = frappe.get_doc(ASN_DOCTYPE, asn_no)

    if not doc.has_permission("write"):
        frappe.throw(_("Not permitted"), frappe.PermissionError)

    if doc.meta.has_field("is_locked") and _as_int(doc.get("is_locked"), 0) == 1:
        frappe.throw(_("ASN is locked."))

    docstatus = _as_int(getattr(doc, "docstatus", 0), 0)
    if docstatus == 2:
        frappe.throw(_("ASN is Cancelled."))
    if docstatus == 1 and not allow_submitted:
        frappe.throw(_("ASN is Submitted. Set allow_submitted = 1 to update receiving quantities."))

    # --- AUTO DETECT REAL FIELDNAMES in child doctype ---
    child_meta = _get_child_meta(doc)

    recvd_field = _resolve_child_field(child_meta, RECV_QTY_CANDIDATES, fallback_label_keywords=["recvd", "received"])
    shipped_field = _resolve_child_field(child_meta, SHIPPED_QTY_CANDIDATES, fallback_label_keywords=["shipped"])

    if not recvd_field:
        frappe.throw(_("Could not detect Received Qty field in child DocType {0}. Candidates tried: {1}")
                    .format(child_meta.name, ", ".join(RECV_QTY_CANDIDATES)))
    if not shipped_field:
        frappe.throw(_("Could not detect Shipped Qty field in child DocType {0}. Candidates tried: {1}")
                    .format(child_meta.name, ", ".join(SHIPPED_QTY_CANDIDATES)))

    updated, skipped, errors = [], [], []

    for i, line in enumerate(lines, start=1):
        try:
            if not isinstance(line, dict):
                raise ValueError(f"Line {i} must be an object/dict")

            child_name = _as_str(line.get("row_name") or line.get("child_name") or line.get("name"))
            item_code = _as_str(line.get("item_code"))
            carton_id = _as_str(line.get("carton_id"))

            qty = _pick_qty(line)

            if mode == "increment" and qty <= 0:
                skipped.append({"line": i, "reason": "qty/received_qty must be > 0 for increment", "line_data": line})
                continue

            row = _find_row(doc, child_name=child_name or None, item_code=item_code or None, carton_id=carton_id or None)
            if not row:
                skipped.append({"line": i, "reason": "row not found (match by name or item_code+carton_id)", "line_data": line})
                continue

            shipped = _as_float(row.get(shipped_field), 0.0)
            current_recvd = _as_float(row.get(recvd_field), 0.0)

            if mode == "increment":
                new_recvd = current_recvd + qty
            else:
                new_recvd = qty  # set mode final

            # optional clamp
            if shipped > 0 and new_recvd > shipped:
                new_recvd = shipped

            row_status = _row_receiving_status(shipped, new_recvd)

            values = {recvd_field: new_recvd}

            # set row statuses if those fields exist
            if child_meta.has_field(F_ROW_RECEIVING_STATUS):
                values[F_ROW_RECEIVING_STATUS] = row_status
            if child_meta.has_field(F_ROW_CARTON_STATUS):
                values[F_ROW_CARTON_STATUS] = row_status

            _db_set(row.doctype, row.name, values)

            updated.append({
                "line": i,
                "row_name": row.name,
                "item_code": row.get(F_ITEM_CODE),
                "carton_id": row.get(F_CARTON_ID),
                "shipped_qty": shipped,
                "prev_recvd_qty": current_recvd,
                "new_recvd_qty": new_recvd,
                "row_status": row_status,
            })

        except Exception as e:
            errors.append({"line": i, "error": str(e), "line_data": line})

    header = {}
    if update_status:
        doc2 = frappe.get_doc(ASN_DOCTYPE, asn_no)
        header = _recalc_header_status(doc2, shipped_field=shipped_field, recvd_field=recvd_field)

        if doc2.meta.has_field(F_ASN_STATUS):
            frappe.db.set_value(ASN_DOCTYPE, asn_no, F_ASN_STATUS, doc2.get(F_ASN_STATUS), update_modified=False)

    frappe.db.commit()

    return {
        "ok": True,
        "api_version": API_VERSION,
        "asn_no": asn_no,
        "docstatus": docstatus,
        "mode": mode,
        "allow_submitted": allow_submitted,
        "resolved_fields": {
            "child_doctype": child_meta.name,
            "shipped_field": shipped_field,
            "recvd_field": recvd_field,
        },
        "updated_count": len(updated),
        "skipped_count": len(skipped),
        "error_count": len(errors),
        "header": header,
        "updated": updated,
        "skipped": skipped,
        "errors": errors,
    }
