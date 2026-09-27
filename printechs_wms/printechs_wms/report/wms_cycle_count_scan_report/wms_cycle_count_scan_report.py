# -*- coding: utf-8 -*-
"""Cycle count scan lines from Task results with optional grouping."""

from __future__ import annotations

import frappe
from frappe import _
from frappe.utils import cint, cstr, flt

TASK_DT = "WMS Cycle Count Task"
RESULT_DT = "WMS Cycle Count Result"
BATCH_DT = "WMS Cycle Count Batch"


def execute(filters=None):
	filters = filters or {}
	group_by = cstr(filters.get("group_by") or "Detail").strip() or "Detail"
	columns = get_columns(group_by)
	rows = fetch_detail_rows(filters)
	totals = compute_audit_totals(rows)
	data = aggregate_rows(rows, group_by)
	data = append_audit_total_row(data, group_by, totals)
	report_summary = get_report_summary(totals)
	return columns, data, None, None, report_summary


def _meta_has(dt: str, fieldname: str) -> bool:
	try:
		return frappe.get_meta(dt).has_field(fieldname)
	except Exception:
		return False


def get_columns(group_by: str):
	if group_by == "Location":
		return [
			{"label": "Location", "fieldname": "location", "fieldtype": "Link", "options": "WMS Bin Location", "width": 180},
			{"label": "Lines", "fieldname": "line_count", "fieldtype": "Int", "width": 80},
			{"label": "Cartons", "fieldname": "carton_count", "fieldtype": "Int", "width": 90},
			{"label": "Items", "fieldname": "item_count", "fieldtype": "Int", "width": 80},
			{"label": "Tasks", "fieldname": "task_count", "fieldtype": "Int", "width": 80},
			{"label": "Total Scanned Qty", "fieldname": "total_counted_qty", "fieldtype": "Float", "width": 140},
		]

	if group_by == "Carton":
		return [
			{"label": "Location", "fieldname": "location", "fieldtype": "Link", "options": "WMS Bin Location", "width": 160},
			{"label": "Carton ID", "fieldname": "carton_id", "fieldtype": "Data", "width": 140},
			{"label": "Lines", "fieldname": "line_count", "fieldtype": "Int", "width": 80},
			{"label": "Items", "fieldname": "item_count", "fieldtype": "Int", "width": 80},
			{"label": "Total Scanned Qty", "fieldname": "total_counted_qty", "fieldtype": "Float", "width": 140},
		]

	if group_by == "Item":
		return [
			{"label": "Item", "fieldname": "item_code", "fieldtype": "Link", "options": "Item", "width": 140},
			{"label": "Item Name", "fieldname": "item_name", "fieldtype": "Data", "width": 220},
			{"label": "Locations", "fieldname": "location_count", "fieldtype": "Int", "width": 90},
			{"label": "Cartons", "fieldname": "carton_count", "fieldtype": "Int", "width": 90},
			{"label": "Lines", "fieldname": "line_count", "fieldtype": "Int", "width": 80},
			{"label": "Total Scanned Qty", "fieldname": "total_counted_qty", "fieldtype": "Float", "width": 140},
		]

	if group_by == "Task":
		return [
			{"label": "Batch", "fieldname": "batch", "fieldtype": "Link", "options": "WMS Cycle Count Batch", "width": 160},
			{"label": "Task", "fieldname": "task", "fieldtype": "Link", "options": "WMS Cycle Count Task", "width": 160},
			{"label": "Count Date", "fieldname": "posting_date", "fieldtype": "Date", "width": 110},
			{"label": "Counted By", "fieldname": "counted_by", "fieldtype": "Link", "options": "User", "width": 160},
			{"label": "Device ID", "fieldname": "device_id", "fieldtype": "Data", "width": 120},
			{"label": "Locations", "fieldname": "location_count", "fieldtype": "Int", "width": 90},
			{"label": "Lines", "fieldname": "line_count", "fieldtype": "Int", "width": 80},
			{"label": "Total Scanned Qty", "fieldname": "total_counted_qty", "fieldtype": "Float", "width": 140},
		]

	return [
		{"label": "Batch", "fieldname": "batch", "fieldtype": "Link", "options": "WMS Cycle Count Batch", "width": 160},
		{"label": "Task", "fieldname": "task", "fieldtype": "Link", "options": "WMS Cycle Count Task", "width": 160},
		{"label": "Count Date", "fieldname": "posting_date", "fieldtype": "Date", "width": 110},
		{"label": "Location", "fieldname": "location", "fieldtype": "Link", "options": "WMS Bin Location", "width": 160},
		{"label": "Carton ID", "fieldname": "carton_id", "fieldtype": "Data", "width": 130},
		{"label": "Item", "fieldname": "item_code", "fieldtype": "Link", "options": "Item", "width": 130},
		{"label": "Item Name", "fieldname": "item_name", "fieldtype": "Data", "width": 200},
		{"label": "Scanned Qty", "fieldname": "counted_qty", "fieldtype": "Float", "width": 110},
		{"label": "System Qty", "fieldname": "system_qty", "fieldtype": "Float", "width": 110},
		{"label": "Delta Qty", "fieldname": "delta_qty", "fieldtype": "Float", "width": 110},
		{"label": "Counted By", "fieldname": "counted_by", "fieldtype": "Link", "options": "User", "width": 160},
		{"label": "Device ID", "fieldname": "device_id", "fieldtype": "Data", "width": 120},
		{"label": "Count Mode", "fieldname": "count_mode", "fieldtype": "Data", "width": 120},
		{"label": "Task Status", "fieldname": "task_status", "fieldtype": "Data", "width": 110},
		{"label": "Batch Status", "fieldname": "batch_status", "fieldtype": "Data", "width": 110},
	]


def fetch_detail_rows(filters: dict) -> list[dict]:
	company = cstr(filters.get("company") or "").strip()
	warehouse = cstr(filters.get("warehouse") or "").strip()
	from_date = filters.get("from_date")
	to_date = filters.get("to_date")
	batch = cstr(filters.get("batch") or "").strip()
	task = cstr(filters.get("task") or "").strip()
	bin_location = cstr(filters.get("bin_location") or "").strip()
	carton_id = cstr(filters.get("carton_id") or "").strip()
	item_code = cstr(filters.get("item_code") or "").strip()
	only_included = cint(filters.get("only_included_in_post") or 0)
	limit = max(1, min(cint(filters.get("limit") or 5000), 50000))

	conditions = ["r.parenttype = %(parenttype)s"]
	params: dict = {"parenttype": TASK_DT, "limit": limit}

	if company:
		conditions.append("t.company = %(company)s")
		params["company"] = company
	if warehouse:
		conditions.append("t.warehouse = %(warehouse)s")
		params["warehouse"] = warehouse
	if from_date:
		conditions.append("t.posting_date >= %(from_date)s")
		params["from_date"] = from_date
	if to_date:
		conditions.append("t.posting_date <= %(to_date)s")
		params["to_date"] = to_date
	if batch and _meta_has(TASK_DT, "batch"):
		conditions.append("t.batch = %(batch)s")
		params["batch"] = batch
	if task:
		conditions.append("t.name = %(task)s")
		params["task"] = task
	if bin_location:
		conditions.append("r.bin_location = %(bin_location)s")
		params["bin_location"] = bin_location
	if carton_id:
		conditions.append("r.carton_id = %(carton_id)s")
		params["carton_id"] = carton_id
	if item_code:
		conditions.append("r.item_code = %(item_code)s")
		params["item_code"] = item_code
	if only_included and _meta_has(TASK_DT, "include_in_post"):
		conditions.append("IFNULL(t.include_in_post, 1) = 1")

	select_extras = []
	if _meta_has(TASK_DT, "batch"):
		select_extras.append("t.batch AS batch")
	else:
		select_extras.append("NULL AS batch")
	if _meta_has(TASK_DT, "counted_by"):
		select_extras.append("t.counted_by AS counted_by")
	else:
		select_extras.append("NULL AS counted_by")
	if _meta_has(TASK_DT, "device_id"):
		select_extras.append("t.device_id AS device_id")
	else:
		select_extras.append("NULL AS device_id")
	if _meta_has(TASK_DT, "count_mode"):
		select_extras.append("t.count_mode AS count_mode")
	else:
		select_extras.append("NULL AS count_mode")

	batch_join = ""
	batch_status_select = "NULL AS batch_status"
	if _meta_has(TASK_DT, "batch"):
		batch_join = f"LEFT JOIN `tab{BATCH_DT}` b ON b.name = t.batch"
		batch_status_select = "b.status AS batch_status"

	where_sql = " AND ".join(conditions)
	extras_sql = ", ".join(select_extras)

	return frappe.db.sql(
		f"""
		SELECT
			{extras_sql},
			t.name AS task,
			t.posting_date,
			t.status AS task_status,
			{batch_status_select},
			r.bin_location AS location,
			r.carton_id,
			r.item_code,
			i.item_name,
			IFNULL(r.counted_qty, 0) AS counted_qty,
			IFNULL(r.system_qty, 0) AS system_qty,
			IFNULL(r.delta_qty, 0) AS delta_qty
		FROM `tab{RESULT_DT}` r
		INNER JOIN `tab{TASK_DT}` t ON t.name = r.parent
		{batch_join}
		LEFT JOIN `tabItem` i ON i.name = r.item_code
		WHERE {where_sql}
		ORDER BY t.posting_date DESC, t.name DESC, r.idx ASC
		LIMIT %(limit)s
		""",
		params,
		as_dict=True,
	)


def compute_audit_totals(rows: list[dict]) -> dict:
	"""Distinct counts + qty sums from raw scan lines (for audit footer/summary)."""
	items: set[str] = set()
	cartons: set[str] = set()
	locations: set[str] = set()
	tasks: set[str] = set()
	total_counted_qty = 0.0
	total_system_qty = 0.0
	total_delta_qty = 0.0

	for row in rows or []:
		item = cstr(row.get("item_code") or "").strip()
		carton = cstr(row.get("carton_id") or "").strip()
		location = cstr(row.get("location") or "").strip()
		task = cstr(row.get("task") or "").strip()
		if item:
			items.add(item)
		if carton:
			cartons.add(carton)
		if location:
			locations.add(location)
		if task:
			tasks.add(task)
		total_counted_qty += flt(row.get("counted_qty"))
		total_system_qty += flt(row.get("system_qty"))
		total_delta_qty += flt(row.get("delta_qty"))

	return {
		"line_count": len(rows or []),
		"distinct_items": len(items),
		"distinct_cartons": len(cartons),
		"distinct_locations": len(locations),
		"distinct_tasks": len(tasks),
		"total_counted_qty": total_counted_qty,
		"total_system_qty": total_system_qty,
		"total_delta_qty": total_delta_qty,
	}


def get_report_summary(totals: dict) -> list[dict]:
	"""Cards shown above the report for quick audit comparison."""
	return [
		{
			"value": totals.get("line_count", 0),
			"label": _("Scan Lines"),
			"datatype": "Int",
			"indicator": "blue",
		},
		{
			"value": totals.get("distinct_items", 0),
			"label": _("Distinct Items"),
			"datatype": "Int",
			"indicator": "green",
		},
		{
			"value": totals.get("distinct_cartons", 0),
			"label": _("Distinct Cartons"),
			"datatype": "Int",
			"indicator": "orange",
		},
		{
			"value": totals.get("total_counted_qty", 0),
			"label": _("Total Scanned Qty"),
			"datatype": "Float",
			"indicator": "red",
		},
	]


def append_audit_total_row(data: list[dict], group_by: str, totals: dict) -> list[dict]:
	"""Append a bold total row so users can reconcile qty vs item counts."""
	if not data:
		return data

	total_label = _("Total")
	row: dict = {"bold": 1}

	if group_by == "Location":
		row.update(
			{
				"location": total_label,
				"line_count": totals.get("line_count", 0),
				"carton_count": totals.get("distinct_cartons", 0),
				"item_count": totals.get("distinct_items", 0),
				"task_count": totals.get("distinct_tasks", 0),
				"total_counted_qty": totals.get("total_counted_qty", 0),
			}
		)
	elif group_by == "Carton":
		row.update(
			{
				"location": total_label,
				"carton_id": "",
				"line_count": totals.get("line_count", 0),
				"item_count": totals.get("distinct_items", 0),
				"total_counted_qty": totals.get("total_counted_qty", 0),
			}
		)
	elif group_by == "Item":
		row.update(
			{
				"item_code": total_label,
				"item_name": "",
				"location_count": totals.get("distinct_locations", 0),
				"carton_count": totals.get("distinct_cartons", 0),
				"line_count": totals.get("line_count", 0),
				"total_counted_qty": totals.get("total_counted_qty", 0),
			}
		)
	elif group_by == "Task":
		row.update(
			{
				"batch": total_label,
				"task": "",
				"location_count": totals.get("distinct_locations", 0),
				"line_count": totals.get("line_count", 0),
				"total_counted_qty": totals.get("total_counted_qty", 0),
			}
		)
	else:
		row.update(
			{
				"batch": total_label,
				"task": "",
				"item_code": "",
				"item_name": _("Distinct items: {0}").format(totals.get("distinct_items", 0)),
				"counted_qty": totals.get("total_counted_qty", 0),
				"system_qty": totals.get("total_system_qty", 0),
				"delta_qty": totals.get("total_delta_qty", 0),
			}
		)

	data.append(row)
	return data


def aggregate_rows(rows: list[dict], group_by: str) -> list[dict]:
	if group_by == "Detail" or not rows:
		return list(rows or [])

	if group_by == "Location":
		buckets: dict[str, dict] = {}
		for row in rows:
			key = cstr(row.get("location") or "").strip() or "(No Location)"
			b = buckets.setdefault(
				key,
				{
					"location": key if key != "(No Location)" else None,
					"line_count": 0,
					"_cartons": set(),
					"_items": set(),
					"_tasks": set(),
					"total_counted_qty": 0.0,
				},
			)
			b["line_count"] += 1
			b["_cartons"].add(cstr(row.get("carton_id") or "").strip())
			b["_items"].add(cstr(row.get("item_code") or "").strip())
			b["_tasks"].add(cstr(row.get("task") or "").strip())
			b["total_counted_qty"] += flt(row.get("counted_qty"))
		out = []
		for b in buckets.values():
			out.append(
				{
					"location": b["location"],
					"line_count": b["line_count"],
					"carton_count": len({x for x in b["_cartons"] if x}),
					"item_count": len({x for x in b["_items"] if x}),
					"task_count": len({x for x in b["_tasks"] if x}),
					"total_counted_qty": b["total_counted_qty"],
				}
			)
		return sorted(out, key=lambda x: cstr(x.get("location") or ""))

	if group_by == "Carton":
		buckets = {}
		for row in rows:
			loc = cstr(row.get("location") or "").strip()
			carton = cstr(row.get("carton_id") or "").strip() or "(No Carton)"
			key = (loc, carton)
			b = buckets.setdefault(
				key,
				{
					"location": loc or None,
					"carton_id": carton if carton != "(No Carton)" else None,
					"line_count": 0,
					"_items": set(),
					"total_counted_qty": 0.0,
				},
			)
			b["line_count"] += 1
			b["_items"].add(cstr(row.get("item_code") or "").strip())
			b["total_counted_qty"] += flt(row.get("counted_qty"))
		out = []
		for b in buckets.values():
			out.append(
				{
					"location": b["location"],
					"carton_id": b["carton_id"],
					"line_count": b["line_count"],
					"item_count": len({x for x in b["_items"] if x}),
					"total_counted_qty": b["total_counted_qty"],
				}
			)
		return sorted(out, key=lambda x: (cstr(x.get("location") or ""), cstr(x.get("carton_id") or "")))

	if group_by == "Item":
		buckets = {}
		for row in rows:
			item = cstr(row.get("item_code") or "").strip() or "(No Item)"
			b = buckets.setdefault(
				item,
				{
					"item_code": item if item != "(No Item)" else None,
					"item_name": row.get("item_name"),
					"line_count": 0,
					"_locations": set(),
					"_cartons": set(),
					"total_counted_qty": 0.0,
				},
			)
			b["line_count"] += 1
			b["_locations"].add(cstr(row.get("location") or "").strip())
			b["_cartons"].add(cstr(row.get("carton_id") or "").strip())
			b["total_counted_qty"] += flt(row.get("counted_qty"))
		out = []
		for b in buckets.values():
			out.append(
				{
					"item_code": b["item_code"],
					"item_name": b["item_name"],
					"location_count": len({x for x in b["_locations"] if x}),
					"carton_count": len({x for x in b["_cartons"] if x}),
					"line_count": b["line_count"],
					"total_counted_qty": b["total_counted_qty"],
				}
			)
		return sorted(out, key=lambda x: cstr(x.get("item_code") or ""))

	if group_by == "Task":
		buckets = {}
		for row in rows:
			task = cstr(row.get("task") or "").strip() or "(No Task)"
			b = buckets.setdefault(
				task,
				{
					"batch": row.get("batch"),
					"task": task if task != "(No Task)" else None,
					"posting_date": row.get("posting_date"),
					"counted_by": row.get("counted_by"),
					"device_id": row.get("device_id"),
					"line_count": 0,
					"_locations": set(),
					"total_counted_qty": 0.0,
				},
			)
			b["line_count"] += 1
			b["_locations"].add(cstr(row.get("location") or "").strip())
			b["total_counted_qty"] += flt(row.get("counted_qty"))
		out = []
		for b in buckets.values():
			out.append(
				{
					"batch": b["batch"],
					"task": b["task"],
					"posting_date": b["posting_date"],
					"counted_by": b["counted_by"],
					"device_id": b["device_id"],
					"location_count": len({x for x in b["_locations"] if x}),
					"line_count": b["line_count"],
					"total_counted_qty": b["total_counted_qty"],
				}
			)
		return sorted(out, key=lambda x: (cstr(x.get("posting_date") or ""), cstr(x.get("task") or "")), reverse=True)

	return rows
