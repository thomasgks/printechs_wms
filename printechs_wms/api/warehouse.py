# Copyright (c) 2026, Printechs and contributors
# License: MIT. See license.txt

"""Compact warehouse master pull API and active-warehouse resolvers for WMS clients."""

from __future__ import annotations

import frappe
from frappe import _
from frappe.utils import cint, now

ACTIVE_WAREHOUSE_FILTERS = {"disabled": 0, "is_group": 0}

DEFAULT_FIELDS = [
	"warehouse_code",
	"name",
	"warehouse_name",
	"company",
	"enabled",
	"updated_on",
]


def _ensure_dict(value):
	if not value:
		return {}
	return frappe.parse_json(value) if isinstance(value, str) else value


def _ensure_list(value):
	if not value:
		return []
	return frappe.parse_json(value) if isinstance(value, str) else value


def _warehouse_has_wms_field(fieldname: str) -> bool:
	return frappe.get_meta("Warehouse").has_field(fieldname)


def merge_active_warehouse_filters(extra: dict | None = None) -> dict:
	filters = dict(ACTIVE_WAREHOUSE_FILTERS)
	if extra:
		filters.update(extra)
	return filters


def is_active_warehouse(docname: str | None) -> bool:
	if not docname or not frappe.db.exists("Warehouse", docname):
		return False

	row = frappe.db.get_value("Warehouse", docname, ["disabled", "is_group"], as_dict=True)
	if not row:
		return False

	return cint(row.get("disabled")) == 0 and cint(row.get("is_group")) == 0


def resolve_warehouse_docname(
	*,
	name: str | None = None,
	code: str | None = None,
	warehouse_name: str | None = None,
	active_only: bool = True,
) -> str | None:
	"""
	Resolve ERP Warehouse docname from docname, code, or warehouse_name.

	When active_only=True (default), disabled and group warehouses are excluded.
	"""
	name = (name or "").strip()
	code = (code or "").strip()
	warehouse_name = (warehouse_name or "").strip()
	base_filters = merge_active_warehouse_filters() if active_only else {}

	if name:
		if active_only:
			if is_active_warehouse(name):
				return name
		elif frappe.db.exists("Warehouse", name):
			return name

	if code:
		wh = frappe.db.get_value("Warehouse", {**base_filters, "code": code}, "name")
		if wh:
			return wh

	for label in (warehouse_name, name):
		if not label:
			continue
		wh = frappe.db.get_value("Warehouse", {**base_filters, "warehouse_name": label}, "name")
		if wh:
			return wh

	if code:
		if active_only:
			if is_active_warehouse(code):
				return code
		elif frappe.db.exists("Warehouse", code):
			return code

	return None


def resolve_warehouse_pair(
	wh_link: str = "",
	wh_code: str = "",
	*,
	active_only: bool = True,
) -> tuple[str, str]:
	"""Return (warehouse_docname, warehouse_code) for ASN/import style callers."""
	wh_link = (wh_link or "").strip()
	wh_code = (wh_code or "").strip()

	docname = resolve_warehouse_docname(
		name=wh_link,
		code=wh_code,
		warehouse_name=wh_link,
		active_only=active_only,
	)
	if not docname:
		return "", ""

	code = frappe.db.get_value("Warehouse", docname, "code") or wh_code or ""
	return docname, code


def resolve_warehouse_or_throw(
	*,
	name: str | None = None,
	code: str | None = None,
	warehouse_name: str | None = None,
	label: str = "ERP Warehouse",
) -> str:
	docname = resolve_warehouse_docname(name=name, code=code, warehouse_name=warehouse_name)
	if docname:
		return docname

	display = (name or warehouse_name or code or "").strip() or "-"
	frappe.throw(_("Could not find active {0}: {1}").format(label, display))


def setup_warehouse_wms_fields():
	"""Ensure Warehouse custom field used for incremental WMS sync."""
	from frappe.custom.doctype.custom_field.custom_field import create_custom_field

	field = {
		"fieldname": "custom_wms_modified",
		"label": "WMS Modified",
		"fieldtype": "Datetime",
		"insert_after": "code" if _warehouse_has_wms_field("code") else "warehouse_name",
		"read_only": 1,
		"no_copy": 1,
	}

	if frappe.db.exists("Custom Field", "Warehouse-custom_wms_modified"):
		return

	create_custom_field("Warehouse", field, ignore_validate=True)


def update_custom_wms_modified(doc, method=None):
	"""Touch WMS sync cursor when a warehouse master record changes."""
	try:
		if not doc or not getattr(doc, "meta", None):
			return
		if doc.meta.name != "Warehouse":
			return
		if not doc.meta.has_field("custom_wms_modified"):
			return
		doc.custom_wms_modified = now()
	except Exception:
		pass


def _compact_warehouse_row(row: dict, fields: list[str]) -> dict:
	payload = {
		"warehouse_code": row.get("code") or row.get("name"),
		"name": row.get("name"),
		"warehouse_name": row.get("warehouse_name"),
		"company": row.get("company"),
		"enabled": cint(row.get("disabled")) == 0 and cint(row.get("is_group")) == 0,
		"updated_on": row.get("custom_wms_modified") or row.get("modified"),
	}

	if not fields:
		return payload
	return {key: payload.get(key) for key in fields if key in payload}


@frappe.whitelist(allow_guest=False)
def get_warehouses_compact(
	filters=None,
	fields=None,
	limit=100,
	offset=0,
	custom_wms_modified_after=None,
	company=None,
):
	"""
	GET/POST /api/method/printechs_wms.api.warehouse.get_warehouses_compact

	Returns active leaf warehouses for WMS sync:
	  - Warehouse.disabled = 0
	  - Warehouse.is_group = 0
	"""
	setup_warehouse_wms_fields()

	filters = _ensure_dict(filters)
	fields = _ensure_list(fields) or DEFAULT_FIELDS
	limit = cint(limit) or 100
	offset = cint(offset) or 0
	company = (company or filters.pop("company", None) or "").strip()

	wh_filters = merge_active_warehouse_filters(filters)
	if company:
		wh_filters["company"] = company

	db_fields = ["name", "warehouse_name", "company", "disabled", "is_group", "modified"]
	if frappe.db.has_column("Warehouse", "code"):
		db_fields.append("code")
	if _warehouse_has_wms_field("custom_wms_modified"):
		db_fields.append("custom_wms_modified")

	if custom_wms_modified_after and _warehouse_has_wms_field("custom_wms_modified"):
		wh_filters["custom_wms_modified"] = (">", custom_wms_modified_after)

	order_by = "custom_wms_modified asc" if _warehouse_has_wms_field("custom_wms_modified") else "modified asc"

	rows = frappe.get_all(
		"Warehouse",
		filters=wh_filters,
		fields=db_fields,
		order_by=order_by,
		limit_start=offset,
		limit_page_length=limit + 1,
	)

	has_more = len(rows) > limit
	if has_more:
		rows = rows[:limit]

	warehouses = [_compact_warehouse_row(row, fields) for row in rows]

	cursor_field = "custom_wms_modified" if _warehouse_has_wms_field("custom_wms_modified") else "modified"
	max_custom_wms_modified = rows[-1].get(cursor_field) if rows else None

	return {
		"warehouses": warehouses,
		"limit": limit,
		"offset": offset,
		"has_more": has_more,
		"max_custom_wms_modified": max_custom_wms_modified,
	}
