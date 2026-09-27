from __future__ import annotations

import frappe
from frappe import _
from frappe.utils import cint

API_VERSION = "asn_receiving_v3"

ASN_DOCTYPE = "WMS ASN"
ASN_ITEM_PARENTFIELD = "items"

# --- common child/header fieldnames ---
F_ITEM_CODE = "item_code"
F_ASN_STATUS = "status"

# child row optional status fields
F_ROW_CARTON_STATUS = "carton_status"
F_ROW_RECEIVING_STATUS = "receiving_status"

# header total fields
F_TOTAL_CTN = "total_ctn"
F_TOTAL_SHIPPED_QTY = "total_shipped_qty"
F_TOTAL_RECEIVED_QTY = "total_received_qty"
F_TOTAL_AMOUNT = "total_amount"
F_TOTAL_AMOUNT_SAR = "total_amount_sar"
F_CONVERSION_RATE = "conversion_rate"

# field detection candidates
CARTON_ID_CANDIDATES = [
    "carton_id",
    "carton_no",
    "carton",
    "carton_code",
    "box_id",
    "box_no",
]

RECV_QTY_CANDIDATES = [
    "recvd_qty",
    "received_qty",
    "qty_received",
    "received",
    "received_quantity",
    "recv_qty",
]

SHIPPED_QTY_CANDIDATES = [
    "shipped_qty",
    "qty_shipped",
    "shipped",
    "shipped_quantity",
]

RATE_CANDIDATES = [
    "unit_cost",
    "rate",
    "valuation_rate",
    "price_list_rate",
    "basic_rate",
    "amount_per_unit",
]


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
    if not isinstance(line, dict):
        return 0.0

    if line.get("qty") is not None:
        return _as_float(line.get("qty"), 0.0)
    if line.get("received_qty") is not None:
        return _as_float(line.get("received_qty"), 0.0)
    if line.get("recvd_qty") is not None:
        return _as_float(line.get("recvd_qty"), 0.0)

    return 0.0


def _resolve_field(meta, candidates: list[str], fallback_label_keywords: list[str] | None = None) -> str | None:
    for f in candidates:
        if meta.has_field(f):
            return f

    if fallback_label_keywords:
        keys = [k.lower() for k in fallback_label_keywords]
        for df in meta.fields:
            lbl = (df.label or "").lower()
            if any(k in lbl for k in keys):
                return df.fieldname

    return None


def _get_child_meta(doc) -> frappe.model.meta.Meta:
    df = doc.meta.get_field(ASN_ITEM_PARENTFIELD)
    if not df or df.fieldtype != "Table" or not df.options:
        frappe.throw(_("Invalid child table config: {0}").format(ASN_ITEM_PARENTFIELD))
    return frappe.get_meta(df.options)


def _row_get(row, fieldname: str | None):
    if not fieldname:
        return None
    return row.get(fieldname)


def _find_rows(doc, *, item_code=None, carton_id=None, carton_field=None):
    rows = doc.get(ASN_ITEM_PARENTFIELD) or []
    item_code = _as_str(item_code)
    carton_id = _as_str(carton_id)

    matched = []
    for r in rows:
        row_item = _as_str(r.get(F_ITEM_CODE))
        row_carton = _as_str(_row_get(r, carton_field))

        if item_code and row_item != item_code:
            continue

        if carton_id and row_carton != carton_id:
            continue

        matched.append(r)

    return matched


def _find_row(doc, *, child_name=None, item_code=None, carton_id=None, carton_field=None):
    """
    Safe match priority:
    1) row_name
    2) item_code + carton_id
    3) item_code only, only if exactly one row exists
    """
    rows = doc.get(ASN_ITEM_PARENTFIELD) or []

    child_name = _as_str(child_name)
    item_code = _as_str(item_code)
    carton_id = _as_str(carton_id)

    if child_name:
        for r in rows:
            if r.name == child_name:
                return r, "row_name"
        return None, "row_name_not_found"

    if item_code and carton_id:
        matches = _find_rows(doc, item_code=item_code, carton_id=carton_id, carton_field=carton_field)
        if len(matches) == 1:
            return matches[0], "item_code+carton_id"
        if len(matches) > 1:
            return None, "multiple_rows_for_item_code+carton_id"
        return None, "row_not_found_for_item_code+carton_id"

    if item_code:
        matches = _find_rows(doc, item_code=item_code, carton_id=None, carton_field=carton_field)
        if len(matches) == 1:
            return matches[0], "item_code_unique"
        if len(matches) > 1:
            return None, "multiple_rows_for_item_code_row_name_or_carton_id_required"
        return None, "row_not_found_for_item_code"

    return None, "insufficient_match_keys"


def _row_receiving_status(shipped: float, recvd: float) -> str:
    if recvd <= 0:
        return "Pending"
    if shipped > 0 and recvd + 1e-9 >= shipped:
        return "Received"
    return "Receiving"


def _asn_header_status(*, any_received: bool, all_received: bool) -> str:
    if all_received:
        return "Completed"
    return "Open"


def _detect_header_total_field(doc, preferred_fieldname: str, label_keywords: list[str]) -> str | None:
    if doc.meta.has_field(preferred_fieldname):
        return preferred_fieldname

    keys = [k.lower() for k in label_keywords]
    for df in doc.meta.fields:
        lbl = (df.label or "").lower()
        if any(k in lbl for k in keys):
            return df.fieldname

    return None


def _get_rate_field(child_meta) -> str | None:
    return _resolve_field(
        child_meta,
        RATE_CANDIDATES,
        fallback_label_keywords=["unit cost", "rate", "valuation"],
    )


def _recalc_header_totals_and_status(
    doc,
    *,
    shipped_field: str,
    recvd_field: str,
    carton_field: str | None,
    rate_field: str | None,
) -> dict:
    total_shipped = 0.0
    total_recvd = 0.0
    total_amount = 0.0

    carton_values = set()
    row_count = 0

    # row-level status evaluation
    all_received = True
    any_received = False

    for r in doc.get(ASN_ITEM_PARENTFIELD) or []:
        row_count += 1

        shipped = _as_float(r.get(shipped_field), 0.0)
        recvd = _as_float(r.get(recvd_field), 0.0)
        rate = _as_float(_row_get(r, rate_field), 0.0)

        total_shipped += shipped
        total_recvd += recvd

        # If business wants amount based on received qty, change shipped -> recvd
        total_amount += shipped * rate

        carton_val = _as_str(_row_get(r, carton_field))
        if carton_val:
            carton_values.add(carton_val)

        if recvd > 0:
            any_received = True

        if recvd + 1e-9 < shipped:
            all_received = False

    total_ctn = len(carton_values) if carton_values else row_count

    conversion_rate = _as_float(doc.get(F_CONVERSION_RATE), 0.0)
    total_amount_sar = total_amount * conversion_rate

    new_status = _asn_header_status(any_received=any_received, all_received=all_received)

    header_updates = {}

    total_ctn_field = _detect_header_total_field(doc, F_TOTAL_CTN, ["total ctn", "ctn"])
    total_shipped_field = _detect_header_total_field(doc, F_TOTAL_SHIPPED_QTY, ["total shipped qty", "shipped qty"])
    total_recvd_field = _detect_header_total_field(doc, F_TOTAL_RECEIVED_QTY, ["total received qty", "received qty", "recvd qty"])
    total_amount_field = _detect_header_total_field(doc, F_TOTAL_AMOUNT, ["total amount"])
    total_amount_sar_field = _detect_header_total_field(doc, F_TOTAL_AMOUNT_SAR, ["total amount (sar)", "amount sar", "total amount sar"])

    if total_ctn_field:
        header_updates[total_ctn_field] = total_ctn
    if total_shipped_field:
        header_updates[total_shipped_field] = total_shipped
    if total_recvd_field:
        header_updates[total_recvd_field] = total_recvd
    if total_amount_field:
        header_updates[total_amount_field] = total_amount
    if total_amount_sar_field:
        header_updates[total_amount_sar_field] = total_amount_sar
    if doc.meta.has_field(F_ASN_STATUS):
        header_updates[F_ASN_STATUS] = new_status

    return {
        "total_ctn": total_ctn,
        "total_shipped": total_shipped,
        "total_recvd": total_recvd,
        "total_amount": total_amount,
        "conversion_rate": conversion_rate,
        "total_amount_sar": total_amount_sar,
        "new_status": new_status,
        "header_updates": header_updates,
        "resolved_header_fields": {
            "total_ctn_field": total_ctn_field,
            "total_shipped_field": total_shipped_field,
            "total_received_field": total_recvd_field,
            "total_amount_field": total_amount_field,
            "total_amount_sar_field": total_amount_sar_field,
            "conversion_rate_field": F_CONVERSION_RATE if doc.meta.has_field(F_CONVERSION_RATE) else None,
            "status_field": F_ASN_STATUS if doc.meta.has_field(F_ASN_STATUS) else None,
        },
    }


def _db_set(doctype: str, name: str, values: dict):
    for fieldname, val in values.items():
        frappe.db.set_value(doctype, name, fieldname, val, update_modified=False)



def _resolve_matched_rows(doc, *, child_name=None, item_code=None, carton_id=None, carton_field=None):
	"""Return matched child rows and a match mode label."""
	child_name = _as_str(child_name)
	item_code = _as_str(item_code)
	carton_id = _as_str(carton_id)

	if child_name:
		row, mode = _find_row(doc, child_name=child_name, carton_field=carton_field)
		return ([row], mode) if row else ([], mode)

	if item_code and carton_id:
		matches = _find_rows(doc, item_code=item_code, carton_id=carton_id, carton_field=carton_field)
		if not matches:
			return [], "row_not_found_for_item_code+carton_id"
		if len(matches) == 1:
			return matches, "item_code+carton_id"
		return matches, "item_code+carton_id+distributed"

	if item_code:
		matches = _find_rows(doc, item_code=item_code, carton_id=None, carton_field=carton_field)
		if len(matches) == 1:
			return matches, "item_code_unique"
		if len(matches) > 1:
			return [], "multiple_rows_for_item_code_row_name_or_carton_id_required"
		return [], "row_not_found_for_item_code"

	return [], "insufficient_match_keys"


def _apply_qty_to_matched_rows(
	rows,
	*,
	shipped_field: str,
	recvd_field: str,
	child_meta,
	qty: float,
	mode: str,
):
	"""Apply increment/set qty across one or more child rows (handles duplicates)."""
	updates = []
	remaining = max(0.0, _as_float(qty, 0.0))
	ordered = sorted(rows, key=lambda r: _as_int(getattr(r, "idx", 0), 0))

	if mode == "set":
		for row in ordered:
			shipped = _as_float(row.get(shipped_field), 0.0)
			alloc = min(shipped, remaining)
			remaining -= alloc
			new_recvd = alloc
			current_recvd = _as_float(row.get(recvd_field), 0.0)
			updates.append((row, current_recvd, new_recvd, shipped))
		return updates

	for row in ordered:
		if remaining <= 0:
			break
		shipped = _as_float(row.get(shipped_field), 0.0)
		current_recvd = _as_float(row.get(recvd_field), 0.0)
		capacity = max(0.0, shipped - current_recvd)
		add = min(capacity, remaining)
		if add <= 0:
			continue
		remaining -= add
		updates.append((row, current_recvd, current_recvd + add, shipped))

	return updates


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
    update_status = _as_int(payload.get("update_status"), 1)
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

    child_meta = _get_child_meta(doc)

    recvd_field = _resolve_field(child_meta, RECV_QTY_CANDIDATES, fallback_label_keywords=["recvd", "received"])
    shipped_field = _resolve_field(child_meta, SHIPPED_QTY_CANDIDATES, fallback_label_keywords=["shipped"])
    carton_field = _resolve_field(child_meta, CARTON_ID_CANDIDATES, fallback_label_keywords=["carton", "box"])
    rate_field = _get_rate_field(child_meta)

    if not recvd_field:
        frappe.throw(
            _("Could not detect Received Qty field in child DocType {0}. Candidates tried: {1}")
            .format(child_meta.name, ", ".join(RECV_QTY_CANDIDATES))
        )

    if not shipped_field:
        frappe.throw(
            _("Could not detect Shipped Qty field in child DocType {0}. Candidates tried: {1}")
            .format(child_meta.name, ", ".join(SHIPPED_QTY_CANDIDATES))
        )

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
                skipped.append({
                    "line": i,
                    "reason": "qty/received_qty must be > 0 for increment",
                    "line_data": line,
                })
                continue

            matched_rows, match_mode = _resolve_matched_rows(
                doc,
                child_name=child_name or None,
                item_code=item_code or None,
                carton_id=carton_id or None,
                carton_field=carton_field,
            )

            if not matched_rows:
                skipped.append({
                    "line": i,
                    "reason": match_mode,
                    "line_data": line,
                })
                continue

            row_updates = _apply_qty_to_matched_rows(
                matched_rows,
                shipped_field=shipped_field,
                recvd_field=recvd_field,
                child_meta=child_meta,
                qty=qty,
                mode=mode,
            )

            if not row_updates:
                skipped.append({
                    "line": i,
                    "reason": "no_capacity_for_qty",
                    "line_data": line,
                })
                continue

            for row, current_recvd, new_recvd, shipped in row_updates:
                row_status = _row_receiving_status(shipped, new_recvd)
                values = {recvd_field: new_recvd}
                if child_meta.has_field(F_ROW_RECEIVING_STATUS):
                    values[F_ROW_RECEIVING_STATUS] = row_status
                if child_meta.has_field(F_ROW_CARTON_STATUS):
                    values[F_ROW_CARTON_STATUS] = row_status
                _db_set(row.doctype, row.name, values)

                updated.append({
                    "line": i,
                    "match_mode": match_mode,
                    "row_name": row.name,
                    "item_code": row.get(F_ITEM_CODE),
                    "carton_id": _row_get(row, carton_field),
                    "shipped_qty": shipped,
                    "prev_recvd_qty": current_recvd,
                    "new_recvd_qty": new_recvd,
                    "row_status": row_status,
                })

        except Exception as e:
            errors.append({
                "line": i,
                "error": str(e),
                "line_data": line,
            })

    header = {}
    if update_status:
        doc2 = frappe.get_doc(ASN_DOCTYPE, asn_no)

        header = _recalc_header_totals_and_status(
            doc2,
            shipped_field=shipped_field,
            recvd_field=recvd_field,
            carton_field=carton_field,
            rate_field=rate_field,
        )

        header_updates = header.get("header_updates") or {}
        if header_updates:
            _db_set(ASN_DOCTYPE, asn_no, header_updates)

    frappe.db.commit()

    final_status = header.get("new_status") or frappe.db.get_value(ASN_DOCTYPE, asn_no, F_ASN_STATUS) or ""

    return {
        "message": {
            "status": final_status,
            "name": asn_no,
        },
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
            "carton_field": carton_field,
            "rate_field": rate_field,
        },
        "updated_count": len(updated),
        "skipped_count": len(skipped),
        "error_count": len(errors),
        "status": final_status,
        "asn_status": final_status,
        "wms_asn_status": final_status,
        "header": header,
        "updated": updated,
        "skipped": skipped,
        "errors": errors,
    }

@frappe.whitelist()
def backfill_asn_received_for_unloaded_cartons(asn_name: str, dry_run: int = 1):
	"""Set received_qty=shipped_qty on open lines when the carton was already unloaded."""
	if not asn_name or not frappe.db.exists(ASN_DOCTYPE, asn_name):
		frappe.throw(_("{0} not found: {1}").format(ASN_DOCTYPE, asn_name))

	doc = frappe.get_doc(ASN_DOCTYPE, asn_name)
	child_meta = _get_child_meta(doc)
	shipped_field = _resolve_field(child_meta, SHIPPED_QTY_CANDIDATES, fallback_label_keywords=["shipped"])
	recvd_field = _resolve_field(child_meta, RECV_QTY_CANDIDATES, fallback_label_keywords=["recvd", "received"])
	carton_field = _resolve_field(child_meta, CARTON_ID_CANDIDATES, fallback_label_keywords=["carton", "box"])
	rate_field = _get_rate_field(child_meta)

	rows = doc.get(ASN_ITEM_PARENTFIELD) or []
	by_carton: dict[str, list] = {}
	for row in rows:
		carton = _as_str(_row_get(row, carton_field))
		if not carton:
			continue
		by_carton.setdefault(carton, []).append(row)

	fixed = []
	for carton, carton_rows in by_carton.items():
		any_received = any(_as_float(r.get(recvd_field), 0.0) > 0 for r in carton_rows)
		if not any_received:
			continue
		for row in carton_rows:
			shipped = _as_float(row.get(shipped_field), 0.0)
			recvd = _as_float(row.get(recvd_field), 0.0)
			if shipped <= 0 or recvd + 1e-9 >= shipped:
				continue
			fixed.append({
				"row_name": row.name,
				"item_code": row.get(F_ITEM_CODE),
				"carton_id": carton,
				"shipped_qty": shipped,
				"prev_recvd_qty": recvd,
				"new_recvd_qty": shipped,
			})

	result = {
		"asn": asn_name,
		"dry_run": bool(cint(dry_run)),
		"lines_fixed": len(fixed),
		"fixed": fixed,
	}

	if cint(dry_run) or not fixed:
		return result

	for item in fixed:
		row_status = _row_receiving_status(item["shipped_qty"], item["new_recvd_qty"])
		values = {recvd_field: item["new_recvd_qty"]}
		if child_meta.has_field(F_ROW_RECEIVING_STATUS):
			values[F_ROW_RECEIVING_STATUS] = row_status
		if child_meta.has_field(F_ROW_CARTON_STATUS):
			values[F_ROW_CARTON_STATUS] = row_status
		_db_set(child_meta.name, item["row_name"], values)

	header = _recalc_header_totals_and_status(
		frappe.get_doc(ASN_DOCTYPE, asn_name),
		shipped_field=shipped_field,
		recvd_field=recvd_field,
		carton_field=carton_field,
		rate_field=rate_field,
	)
	if header.get("header_updates"):
		_db_set(ASN_DOCTYPE, asn_name, header["header_updates"])

	frappe.db.commit()
	result["header"] = header
	return result
