# -*- coding: utf-8 -*-
"""WMS stock movement detail by location and carton with opening/closing summary."""

from __future__ import annotations

import frappe
from frappe import _
from frappe.utils import cint, cstr, flt, get_datetime

LEDGER_DT = "WMS Stock Ledger Entry"
BIN_LOCATION_DT = "WMS Bin Location"


def execute(filters=None):
	filters = filters or {}
	columns = get_columns(filters)
	data = get_data(filters)
	report_summary = get_report_summary(filters, data)
	return columns, data, None, None, report_summary


def _meta_has(dt: str, fieldname: str) -> bool:
	try:
		return frappe.get_meta(dt).has_field(fieldname)
	except Exception:
		return False


def get_columns(filters=None):
	columns = [
		{"label": _("Posting Datetime"), "fieldname": "posting_datetime", "fieldtype": "Datetime", "width": 170},
		{"label": _("Company"), "fieldname": "company", "fieldtype": "Link", "options": "Company", "width": 140},
		{"label": _("Warehouse"), "fieldname": "warehouse", "fieldtype": "Link", "options": "Warehouse", "width": 200},
		{"label": _("Item"), "fieldname": "item_code", "fieldtype": "Link", "options": "Item", "width": 130},
		{"label": _("Item Name"), "fieldname": "item_name", "fieldtype": "Data", "width": 220},
		{"label": _("Location"), "fieldname": "location", "fieldtype": "Link", "options": "WMS Bin Location", "width": 150},
		{"label": _("Carton"), "fieldname": "carton", "fieldtype": "Data", "width": 140},
		{"label": _("Qty Before"), "fieldname": "qty_before", "fieldtype": "Float", "width": 100},
		{"label": _("Qty Change"), "fieldname": "qty_change", "fieldtype": "Float", "width": 100},
		{"label": _("Qty After"), "fieldname": "qty_after", "fieldtype": "Float", "width": 100},
		{"label": _("Event Type"), "fieldname": "event_type", "fieldtype": "Data", "width": 110},
		{"label": _("Voucher Type"), "fieldname": "voucher_doctype", "fieldtype": "Data", "width": 120},
		{
			"label": _("Voucher"),
			"fieldname": "voucher_name",
			"fieldtype": "Dynamic Link",
			"options": "voucher_doctype",
			"width": 170,
		},
	]

	if _meta_has("Stock Entry", "custom_material_request"):
		columns.append(
			{
				"label": _("Material Request"),
				"fieldname": "material_request",
				"fieldtype": "Link",
				"options": "Material Request",
				"width": 170,
			}
		)

	columns.extend(
		[
			{"label": _("WMS Txn ID"), "fieldname": "wms_txn_id", "fieldtype": "Data", "width": 170},
			{"label": _("Remarks"), "fieldname": "remarks", "fieldtype": "Data", "width": 260},
		]
	)
	return columns


def _build_where(filters: dict):
	where = ["sle.company = %(company)s"]
	params = {"company": filters["company"]}

	for key in ("item_code", "event_type", "location", "carton"):
		value = cstr(filters.get(key) or "").strip()
		if value:
			where.append(f"sle.{key} = %({key})s")
			params[key] = value

	if filters.get("from_date"):
		where.append("sle.posting_datetime >= %(from_date)s")
		params["from_date"] = get_datetime(filters["from_date"])

	if filters.get("to_date"):
		where.append("sle.posting_datetime <= %(to_date)s")
		params["to_date"] = get_datetime(filters["to_date"])

	warehouse = cstr(filters.get("warehouse") or "").strip()
	if warehouse:
		where.append("bl.erp_warehouse = %(warehouse)s")
		params["warehouse"] = warehouse

	return where, params


def _get_limit(filters: dict, default: int = 2000, max_limit: int = 20000) -> int:
	limit = cint(filters.get("limit") or default)
	if limit <= 0:
		limit = default
	return min(limit, max_limit)


def get_data(filters: dict):
	company = cstr(filters.get("company") or "").strip()
	if not company:
		frappe.throw(_("Company is required"))

	where, params = _build_where(filters)
	limit = _get_limit(filters)
	mr_select = ""
	mr_join = ""

	if _meta_has("Stock Entry", "custom_material_request"):
		mr_select = ", se.custom_material_request AS material_request"
		mr_join = """
			LEFT JOIN `tabStock Entry` se
				ON se.name = sle.voucher_name
				AND sle.voucher_doctype = 'Stock Entry'
		"""

	sql = f"""
		SELECT
			sle.posting_datetime,
			sle.company,
			bl.erp_warehouse AS warehouse,
			sle.item_code,
			i.item_name,
			sle.location,
			IFNULL(sle.carton, '') AS carton,
			(IFNULL(sle.qty_after, 0) - IFNULL(sle.qty_change, 0)) AS qty_before,
			sle.qty_change,
			sle.qty_after,
			sle.event_type,
			sle.voucher_doctype,
			sle.voucher_name,
			sle.wms_txn_id,
			sle.remarks
			{mr_select}
		FROM `tabWMS Stock Ledger Entry` sle
		LEFT JOIN `tabWMS Bin Location` bl ON bl.name = sle.location
		LEFT JOIN `tabItem` i ON i.name = sle.item_code
		{mr_join}
		WHERE {" AND ".join(where)}
		ORDER BY sle.posting_datetime ASC, sle.name ASC
		LIMIT {limit}
	"""
	return frappe.db.sql(sql, params, as_dict=True)


def _sum_qty_change(rows: list[dict]) -> dict:
	total_in = 0.0
	total_out = 0.0
	for row in rows:
		change = flt(row.get("qty_change"))
		if change > 0:
			total_in += change
		elif change < 0:
			total_out += abs(change)
	return {
		"total_in": total_in,
		"total_out": total_out,
		"net_change": total_in - total_out,
	}


def _get_opening_balance(filters: dict) -> float | None:
	item_code = cstr(filters.get("item_code") or "").strip()
	warehouse = cstr(filters.get("warehouse") or "").strip()
	from_date = filters.get("from_date")

	if not (item_code and warehouse and from_date):
		return None

	params = {
		"company": filters["company"],
		"item_code": item_code,
		"warehouse": warehouse,
		"from_date": get_datetime(from_date),
	}

	rows = frappe.db.sql(
		"""
		SELECT sub.qty_after
		FROM (
			SELECT
				sle.qty_after,
				ROW_NUMBER() OVER (
					PARTITION BY sle.item_code, sle.location, IFNULL(sle.carton, '')
					ORDER BY sle.posting_datetime DESC, sle.name DESC
				) AS rn
			FROM `tabWMS Stock Ledger Entry` sle
			INNER JOIN `tabWMS Bin Location` bl ON bl.name = sle.location
			WHERE sle.company = %(company)s
				AND sle.item_code = %(item_code)s
				AND bl.erp_warehouse = %(warehouse)s
				AND sle.posting_datetime < %(from_date)s
		) sub
		WHERE sub.rn = 1
		""",
		params,
		as_dict=True,
	)
	return sum(flt(row.qty_after) for row in rows)


def _get_wms_current_balance(filters: dict) -> float | None:
	item_code = cstr(filters.get("item_code") or "").strip()
	warehouse = cstr(filters.get("warehouse") or "").strip()
	if not (item_code and warehouse):
		return None

	rows = frappe.db.sql(
		"""
		SELECT SUM(IFNULL(sb.qty, 0)) AS qty
		FROM `tabWMS Stock Balance` sb
		WHERE sb.company = %(company)s
			AND sb.warehouse = %(warehouse)s
			AND sb.item_code = %(item_code)s
		""",
		{
			"company": filters["company"],
			"item_code": item_code,
			"warehouse": warehouse,
		},
		as_dict=True,
	)
	return flt(rows[0].qty) if rows else 0.0


def _get_erp_bin_qty(filters: dict) -> float | None:
	item_code = cstr(filters.get("item_code") or "").strip()
	warehouse = cstr(filters.get("warehouse") or "").strip()
	if not (item_code and warehouse):
		return None

	return flt(
		frappe.db.get_value(
			"Bin",
			{"item_code": item_code, "warehouse": warehouse},
			"actual_qty",
		)
	)


def get_report_summary(filters: dict, data: list[dict]) -> list[dict]:
	movement = _sum_qty_change(data)
	summary = [
		{
			"value": movement["total_in"],
			"label": _("Total In"),
			"datatype": "Float",
			"indicator": "green",
		},
		{
			"value": movement["total_out"],
			"label": _("Total Out"),
			"datatype": "Float",
			"indicator": "red",
		},
		{
			"value": movement["net_change"],
			"label": _("Net Change"),
			"datatype": "Float",
			"indicator": "blue",
		},
		{
			"value": len(data),
			"label": _("Movement Lines"),
			"datatype": "Int",
			"indicator": "grey",
		},
	]

	opening = _get_opening_balance(filters)
	if opening is not None:
		closing = opening + movement["net_change"]
		summary.insert(
			0,
			{
				"value": opening,
				"label": _("Opening Balance"),
				"datatype": "Float",
				"indicator": "blue",
			},
		)
		summary.insert(
			4,
			{
				"value": closing,
				"label": _("Closing Balance"),
				"datatype": "Float",
				"indicator": "green",
			},
		)

	wms_current = _get_wms_current_balance(filters)
	if wms_current is not None:
		summary.append(
			{
				"value": wms_current,
				"label": _("WMS Current (Warehouse)"),
				"datatype": "Float",
				"indicator": "orange",
			}
		)

	erp_qty = _get_erp_bin_qty(filters)
	if erp_qty is not None:
		summary.append(
			{
				"value": erp_qty,
				"label": _("ERP Bin Qty"),
				"datatype": "Float",
				"indicator": "purple",
			}
		)

	return summary
