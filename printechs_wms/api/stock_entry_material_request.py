# -*- coding: utf-8 -*-
"""Link Material Request to Stock Entry header and item lines."""

from __future__ import annotations

import frappe
from frappe import _
from frappe.utils import cint, cstr


HEADER_FIELD = "custom_material_request"


def has_header_field() -> bool:
	try:
		return bool(frappe.db.has_column("Stock Entry", HEADER_FIELD))
	except Exception:
		return False


def resolve_mr_item_name(material_request: str, item_code: str) -> str | None:
	if not material_request or not item_code:
		return None
	if not frappe.db.exists("Material Request", material_request):
		return None
	mr = frappe.get_doc("Material Request", material_request)
	for row in mr.items:
		if row.item_code == item_code:
			return row.name
	return None


def resolve_material_request(material_request=None, items=None) -> str:
	mr = cstr(material_request)
	if mr:
		return mr

	seen: set[str] = set()
	for row in items or []:
		if not isinstance(row, dict):
			continue
		row_mr = cstr(row.get("material_request"))
		if row_mr:
			seen.add(row_mr)
	if len(seen) == 1:
		return next(iter(seen))
	return ""


def apply_material_request_to_stock_entry(stock_entry_doc, material_request=None, items=None) -> dict:
	"""Set header custom_material_request and line MR links before insert."""
	if not has_header_field():
		return {"updated": False, "reason": "field_missing"}

	mr = resolve_material_request(material_request, items)
	if not mr:
		return {"updated": False, "reason": "no_material_request"}
	if not frappe.db.exists("Material Request", mr):
		frappe.throw(_("Material Request not found: {0}").format(mr))

	stock_entry_doc.set(HEADER_FIELD, mr)

	item_row_map: dict[str, dict] = {}
	for row in items or []:
		if isinstance(row, dict):
			code = cstr(row.get("item_code"))
			if code:
				item_row_map[code] = row

	for line in stock_entry_doc.items or []:
		if not cstr(getattr(line, "material_request", None)):
			line.material_request = mr

		if not cstr(getattr(line, "material_request_item", None)):
			payload_row = item_row_map.get(line.item_code) or {}
			mr_item = cstr(payload_row.get("material_request_item"))
			if not mr_item:
				mr_item = resolve_mr_item_name(mr, line.item_code) or ""
			if mr_item:
				line.material_request_item = mr_item

	return {"updated": True, "material_request": mr}


def infer_material_request_from_lines(stock_entry_name: str) -> str:
	rows = frappe.db.sql(
		"""
		SELECT DISTINCT material_request
		FROM `tabStock Entry Detail`
		WHERE parent = %s AND IFNULL(material_request, '') != ''
		""",
		stock_entry_name,
		as_list=True,
	)
	values = [cstr(row[0]) for row in rows if row and row[0]]
	if len(values) == 1:
		return values[0]
	return values[0] if values else ""


def copy_material_request_header_from_stock_entry(target_doc, source_name: str) -> dict:
	if not has_header_field() or not source_name:
		return {"updated": False, "reason": "field_missing"}

	mr = cstr(frappe.db.get_value("Stock Entry", source_name, HEADER_FIELD))
	if not mr:
		mr = infer_material_request_from_lines(source_name)
	if not mr:
		return {"updated": False, "reason": "no_material_request"}

	target_doc.set(HEADER_FIELD, mr)
	for line in target_doc.items or []:
		if not cstr(getattr(line, "material_request", None)):
			line.material_request = mr
		if not cstr(getattr(line, "material_request_item", None)):
			mr_item = resolve_mr_item_name(mr, line.item_code)
			if mr_item:
				line.material_request_item = mr_item

	return {"updated": True, "material_request": mr}


def _resolve_header_material_request(stock_entry_name: str, source_name: str | None = None) -> str:
	mr = infer_material_request_from_lines(stock_entry_name)
	if mr:
		return mr
	if source_name:
		mr = cstr(frappe.db.get_value("Stock Entry", source_name, HEADER_FIELD))
		if mr:
			return mr
		return infer_material_request_from_lines(source_name)
	return ""


def ensure_stock_entry_material_request_header(
	stock_entry_name: str,
	*,
	source_name: str | None = None,
	commit: bool = False,
) -> dict:
	"""Backfill header from line-level material_request when missing."""
	if not has_header_field() or not stock_entry_name:
		return {"updated": False, "stock_entry": stock_entry_name, "reason": "field_missing"}

	current = cstr(frappe.db.get_value("Stock Entry", stock_entry_name, HEADER_FIELD))
	if current:
		return {
			"updated": False,
			"stock_entry": stock_entry_name,
			"material_request": current,
			"reason": "already_set",
		}

	mr = _resolve_header_material_request(stock_entry_name, source_name)
	if not mr:
		return {"updated": False, "stock_entry": stock_entry_name, "reason": "no_line_mr"}

	frappe.db.set_value("Stock Entry", stock_entry_name, HEADER_FIELD, mr, update_modified=False)
	if commit:
		frappe.db.commit()

	return {"updated": True, "stock_entry": stock_entry_name, "material_request": mr}


@frappe.whitelist()
def backfill_stock_entry_material_request_headers(limit=500, dry_run=1, only_wms=1):
	"""Backfill Stock Entry.custom_material_request from line-level MR links."""
	if not has_header_field():
		frappe.throw(_("Stock Entry.{0} field is not installed").format(HEADER_FIELD))

	limit = max(1, min(cint(limit) or 500, 5000))
	dry_run = cint(dry_run)
	only_wms = cint(only_wms)

	conditions = [
		"se.docstatus < 2",
		"IFNULL(se.custom_material_request, '') = ''",
		"IFNULL(sed.material_request, '') != ''",
	]
	if only_wms:
		conditions.append("(se.remarks LIKE '%[EXT:%' OR se.remarks LIKE '%[EXTREF:%' OR se.remarks LIKE '%Push from WMS%')")

	rows = frappe.db.sql(
		f"""
		SELECT DISTINCT se.name
		FROM `tabStock Entry` se
		INNER JOIN `tabStock Entry Detail` sed ON sed.parent = se.name
		WHERE {" AND ".join(conditions)}
		ORDER BY se.modified DESC
		LIMIT {limit}
		""",
		as_dict=True,
	)

	updated = []
	skipped = []
	for row in rows:
		name = row["name"]
		if dry_run:
			mr = infer_material_request_from_lines(name)
			updated.append({"stock_entry": name, "material_request": mr, "dry_run": True})
			continue
		result = ensure_stock_entry_material_request_header(name)
		if result.get("updated"):
			updated.append(result)
		else:
			skipped.append(result)

	if not dry_run:
		frappe.db.commit()

	return {
		"ok": True,
		"dry_run": bool(dry_run),
		"only_wms": bool(only_wms),
		"candidates": len(rows),
		"updated_count": len(updated),
		"updated": updated,
		"skipped": skipped,
	}
