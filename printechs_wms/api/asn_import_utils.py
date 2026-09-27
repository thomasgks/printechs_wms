# -*- coding: utf-8 -*-
"""Shared ASN Excel import helpers for carton totals and validation."""

from __future__ import annotations

import frappe
from frappe.utils import cint, cstr, flt


def _s(v) -> str:
	return cstr(v or "").strip()



def _row_carton_id(row: dict) -> str:
	return _s(row.get("carton_id") or row.get("carton") or row.get("box_id"))


def merge_duplicate_asn_item_rows(item_list: list[dict]) -> tuple[list[dict], dict]:
	"""Merge Excel rows sharing the same item_code + carton_id by summing qty fields."""
	original_count = len(item_list or [])
	merged_map: dict[tuple[str, str], dict] = {}
	order: list[tuple[str, str]] = []
	sum_fields = ("shipped_qty", "qty", "received_qty", "extended_cost")

	for row in item_list or []:
		if not isinstance(row, dict):
			continue
		item_code = _s(row.get("item_code"))
		if not item_code:
			continue
		key = (item_code, _row_carton_id(row))
		if key not in merged_map:
			merged_map[key] = dict(row)
			order.append(key)
			continue

		target = merged_map[key]
		for field in sum_fields:
			if row.get(field) in (None, ""):
				continue
			target[field] = flt(target.get(field)) + flt(row.get(field))

	merged_list = [merged_map[key] for key in order]
	return merged_list, {
		"original_rows": original_count,
		"merged_rows": len(merged_list),
		"merged_duplicate_groups": max(0, original_count - len(merged_list)),
	}


def summarize_item_rows(item_list: list[dict]) -> dict:
	"""Summarize importable ASN item rows from Excel."""
	cartons: set[str] = set()
	lines = 0
	shipped_qty = 0.0
	blank_carton_lines = 0
	skipped: dict[str, int] = {}

	for it in item_list or []:
		if not isinstance(it, dict):
			continue
		item_code = _s(it.get("item_code"))
		qty = it.get("shipped_qty")
		if qty in (None, "") and it.get("qty") not in (None, ""):
			qty = it.get("qty")
		if not item_code:
			skipped["no_item_code"] = skipped.get("no_item_code", 0) + 1
			continue
		if qty in (None, ""):
			skipped["no_qty"] = skipped.get("no_qty", 0) + 1
			continue

		carton = _s(it.get("carton_id") or it.get("carton") or it.get("box_id"))
		if not carton:
			blank_carton_lines += 1
		else:
			cartons.add(carton)

		lines += 1
		shipped_qty += flt(qty)

	return {
		"item_lines": lines,
		"distinct_cartons": len(cartons),
		"total_shipped_qty": shipped_qty,
		"blank_carton_lines": blank_carton_lines,
		"skipped": skipped,
	}


def apply_header_totals(asn_doc, header_row: dict, item_stats: dict) -> dict:
	"""Set total_ctn / total_shipped_qty on ASN header and return audit info."""
	audit = {
		"excel_total_ctn": None,
		"imported_distinct_cartons": item_stats.get("distinct_cartons", 0),
		"imported_item_lines": item_stats.get("item_lines", 0),
		"imported_total_shipped_qty": item_stats.get("total_shipped_qty", 0),
		"blank_carton_lines": item_stats.get("blank_carton_lines", 0),
		"mismatch": False,
		"warnings": [],
	}

	excel_total = header_row.get("total_ctn")
	if excel_total in (None, "") and header_row.get("total_carton") not in (None, ""):
		excel_total = header_row.get("total_carton")
	if excel_total in (None, "") and header_row.get("total_cartons") not in (None, ""):
		excel_total = header_row.get("total_cartons")

	if excel_total not in (None, ""):
		audit["excel_total_ctn"] = cint(excel_total)

	computed_cartons = cint(item_stats.get("distinct_cartons") or 0)
	computed_qty = flt(item_stats.get("total_shipped_qty") or 0)

	if audit["excel_total_ctn"] is not None and audit["excel_total_ctn"] != computed_cartons:
		audit["mismatch"] = True
		audit["warnings"].append(
			f"Header total_ctn ({audit['excel_total_ctn']}) != imported distinct cartons ({computed_cartons})"
		)

	if item_stats.get("blank_carton_lines"):
		audit["warnings"].append(
			f"{item_stats['blank_carton_lines']} item line(s) have blank carton_id"
		)

	if asn_doc.meta.has_field("total_ctn"):
		asn_doc.total_ctn = audit["excel_total_ctn"] if audit["excel_total_ctn"] is not None else computed_cartons
		audit["saved_total_ctn"] = asn_doc.total_ctn

	if asn_doc.meta.has_field("total_shipped_qty") and computed_qty:
		asn_doc.total_shipped_qty = computed_qty
		audit["saved_total_shipped_qty"] = asn_doc.total_shipped_qty

	return audit


def get_asn_carton_audit(asn_name: str) -> dict:
	"""Compare ASN header totals with imported item/carton lines."""
	if not asn_name or not frappe.db.exists("WMS ASN", asn_name):
		frappe.throw(f"WMS ASN not found: {asn_name}")

	header_total_ctn = cint(frappe.db.get_value("WMS ASN", asn_name, "total_ctn") or 0)
	rows = frappe.db.sql(
		"""
		SELECT
			COUNT(name) AS item_lines,
			COUNT(DISTINCT NULLIF(carton_id, '')) AS distinct_cartons,
			SUM(CASE WHEN IFNULL(carton_id, '') = '' THEN 1 ELSE 0 END) AS blank_carton_lines,
			SUM(IFNULL(shipped_qty, 0)) AS total_shipped_qty
		FROM `tabWMS ASN Item`
		WHERE parent = %s
		""",
		asn_name,
		as_dict=True,
	)[0]

	distinct_cartons = cint(rows.distinct_cartons or 0)
	return {
		"asn": asn_name,
		"header_total_ctn": header_total_ctn,
		"imported_distinct_cartons": distinct_cartons,
		"imported_item_lines": cint(rows.item_lines or 0),
		"blank_carton_lines": cint(rows.blank_carton_lines or 0),
		"total_shipped_qty": flt(rows.total_shipped_qty or 0),
		"cartons_match_header": header_total_ctn == distinct_cartons if header_total_ctn else None,
		"all_cartons_present": distinct_cartons > 0 and cint(rows.blank_carton_lines or 0) == 0,
	}


@frappe.whitelist()
def validate_asn_carton_counts(asn_name: str):
	return get_asn_carton_audit(asn_name)


@frappe.whitelist()
def merge_asn_duplicate_item_lines(asn_name: str, dry_run: int = 1):
	"""Merge existing ASN child rows with the same item_code + carton_id (data repair)."""
	from printechs_wms.api.asn_receiving import (
		ASN_DOCTYPE,
		ASN_ITEM_PARENTFIELD,
		_get_child_meta,
		_db_set,
		_get_rate_field,
		_recalc_header_totals_and_status,
		_resolve_field,
		CARTON_ID_CANDIDATES,
		RECV_QTY_CANDIDATES,
		SHIPPED_QTY_CANDIDATES,
	)

	if not asn_name or not frappe.db.exists(ASN_DOCTYPE, asn_name):
		frappe.throw(f"WMS ASN not found: {asn_name}")

	doc = frappe.get_doc(ASN_DOCTYPE, asn_name)
	child_meta = _get_child_meta(doc)
	shipped_field = _resolve_field(child_meta, SHIPPED_QTY_CANDIDATES, fallback_label_keywords=["shipped"])
	recvd_field = _resolve_field(child_meta, RECV_QTY_CANDIDATES, fallback_label_keywords=["recvd", "received"])
	carton_field = _resolve_field(child_meta, CARTON_ID_CANDIDATES, fallback_label_keywords=["carton", "box"])
	rate_field = _get_rate_field(child_meta)

	rows = list(doc.get(ASN_ITEM_PARENTFIELD) or [])
	grouped: dict[tuple[str, str], list] = {}
	singles: list = []
	for row in rows:
		key = (_s(row.get("item_code")), _s(getattr(row, "carton_id", None) or row.get(carton_field) if carton_field else None))
		if carton_field:
			key = (_s(row.get("item_code")), _s(row.get(carton_field)))
		else:
			key = (_s(row.get("item_code")), "")
		if not key[0]:
			singles.append(row)
			continue
		grouped.setdefault(key, []).append(row)

	merged_rows = []
	deleted = []
	for dup_rows in grouped.values():
		if len(dup_rows) == 1:
			merged_rows.append(dup_rows[0])
			continue
		keep = dup_rows[0]
		for extra in dup_rows[1:]:
			keep.set(shipped_field, flt(keep.get(shipped_field)) + flt(extra.get(shipped_field)))
			keep.set(recvd_field, flt(keep.get(recvd_field)) + flt(extra.get(recvd_field)))
			if rate_field and extra.get(rate_field) not in (None, ""):
				if keep.get(rate_field) in (None, ""):
					keep.set(rate_field, extra.get(rate_field))
			deleted.append(extra.name)
		merged_rows.append(keep)

	merged_rows.extend(singles)
	result = {
		"asn": asn_name,
		"dry_run": bool(cint(dry_run)),
		"duplicate_groups": sum(1 for rows in grouped.values() if len(rows) > 1),
		"rows_deleted": len(deleted),
		"rows_before": len(rows),
		"rows_after": len(merged_rows),
	}

	if cint(dry_run):
		return result

	doc.set(ASN_ITEM_PARENTFIELD, merged_rows)
	doc.flags.ignore_validate_update_after_submit = True
	doc.save(ignore_permissions=True)
	for name in deleted:
		frappe.delete_doc(child_meta.name, name, force=1, ignore_permissions=True)

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
