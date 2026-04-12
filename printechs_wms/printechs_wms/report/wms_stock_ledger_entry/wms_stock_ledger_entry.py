# -*- coding: utf-8 -*-
from __future__ import annotations

import frappe
from frappe.utils import cint


def execute(filters=None):
    filters = filters or {}

    # If user selected item_code -> show "history view"
    if filters.get("item_code"):
        columns = get_item_history_columns()
        data = get_item_history_data(filters)
        return columns, data

    # Otherwise show normal ledger view
    columns = get_log_columns()
    data = get_log_data(filters)
    return columns, data


# ============================================================
# COMMON WHERE BUILDER
# ============================================================

def _build_where(filters: dict):
    where = ["1=1"]
    params = {}

    # main filters
    for k in ("item_code", "event_type", "location", "carton"):
        v = filters.get(k)
        if v:
            where.append(f"`tabWMS Stock Ledger Entry`.`{k}` = %({k})s")
            params[k] = v

    # date range
    if filters.get("from_date"):
        where.append("`tabWMS Stock Ledger Entry`.`posting_datetime` >= %(from_date)s")
        params["from_date"] = filters["from_date"]

    if filters.get("to_date"):
        where.append("`tabWMS Stock Ledger Entry`.`posting_datetime` <= %(to_date)s")
        params["to_date"] = filters["to_date"]

    return where, params


def _get_limit(filters, default=500, max_limit=5000):
    limit = cint(filters.get("limit") or default)
    if limit <= 0:
        limit = default
    if limit > max_limit:
        limit = max_limit
    return limit


# ============================================================
# VIEW 1: FULL LOG (DEFAULT)
# ============================================================

def get_log_columns():
    return [
        {"label": "Posting Datetime", "fieldname": "posting_datetime", "fieldtype": "Datetime", "width": 170},
        {"label": "Item Code", "fieldname": "item_code", "fieldtype": "Link", "options": "Item", "width": 140},
        {"label": "Location", "fieldname": "location", "fieldtype": "Data", "width": 140},
        {"label": "Carton", "fieldname": "carton", "fieldtype": "Data", "width": 170},
        {"label": "Qty Change", "fieldname": "qty_change", "fieldtype": "Float", "width": 110},
        {"label": "Qty After", "fieldname": "qty_after", "fieldtype": "Float", "width": 110},
        {"label": "Event Type", "fieldname": "event_type", "fieldtype": "Data", "width": 130},
        {"label": "WMS Txn ID", "fieldname": "wms_txn_id", "fieldtype": "Data", "width": 180},
        {"label": "Remarks", "fieldname": "remarks", "fieldtype": "Data", "width": 260},
    ]


def get_log_data(filters):
    where, params = _build_where(filters)
    limit = _get_limit(filters, default=500, max_limit=5000)

    sql = f"""
        SELECT
            `tabWMS Stock Ledger Entry`.`posting_datetime`,
            `tabWMS Stock Ledger Entry`.`item_code`,
            `tabWMS Stock Ledger Entry`.`location`,
            `tabWMS Stock Ledger Entry`.`carton`,
            `tabWMS Stock Ledger Entry`.`qty_change`,
            `tabWMS Stock Ledger Entry`.`qty_after`,
            `tabWMS Stock Ledger Entry`.`event_type`,
            `tabWMS Stock Ledger Entry`.`wms_txn_id`,
            `tabWMS Stock Ledger Entry`.`remarks`
        FROM `tabWMS Stock Ledger Entry`
        WHERE {" AND ".join(where)}
        ORDER BY `tabWMS Stock Ledger Entry`.`posting_datetime` DESC,
                 `tabWMS Stock Ledger Entry`.`name` DESC
        LIMIT {limit}
    """
    return frappe.db.sql(sql, params, as_dict=True)


# ============================================================
# VIEW 2: ITEM TRANSACTION HISTORY (WHEN item_code IS SELECTED)
# ============================================================

def get_item_history_columns():
    return [
        {"label": "Transaction Date", "fieldname": "posting_datetime", "fieldtype": "Datetime", "width": 180},
        {"label": "Event Type", "fieldname": "event_type", "fieldtype": "Data", "width": 140},
        {"label": "Transaction ID", "fieldname": "wms_txn_id", "fieldtype": "Data", "width": 200},
        {"label": "Remarks", "fieldname": "remarks", "fieldtype": "Data", "width": 420},
    ]


def get_item_history_data(filters):
    # force item_code filter exists here
    where = ["`tabWMS Stock Ledger Entry`.`item_code` = %(item_code)s"]
    params = {"item_code": filters["item_code"]}

    # OPTIONAL: allow narrowing for history view too
    if filters.get("event_type"):
        where.append("`tabWMS Stock Ledger Entry`.`event_type` = %(event_type)s")
        params["event_type"] = filters["event_type"]

    if filters.get("location"):
        where.append("`tabWMS Stock Ledger Entry`.`location` = %(location)s")
        params["location"] = filters["location"]

    if filters.get("carton"):
        where.append("`tabWMS Stock Ledger Entry`.`carton` = %(carton)s")
        params["carton"] = filters["carton"]

    if filters.get("from_date"):
        where.append("`tabWMS Stock Ledger Entry`.`posting_datetime` >= %(from_date)s")
        params["from_date"] = filters["from_date"]

    if filters.get("to_date"):
        where.append("`tabWMS Stock Ledger Entry`.`posting_datetime` <= %(to_date)s")
        params["to_date"] = filters["to_date"]

    # history can be bigger, but keep safe limit
    limit = _get_limit(filters, default=2000, max_limit=20000)

    sql = f"""
        SELECT
            `tabWMS Stock Ledger Entry`.`posting_datetime`,
            `tabWMS Stock Ledger Entry`.`event_type`,
            `tabWMS Stock Ledger Entry`.`wms_txn_id`,
            `tabWMS Stock Ledger Entry`.`remarks`
        FROM `tabWMS Stock Ledger Entry`
        WHERE {" AND ".join(where)}
        ORDER BY `tabWMS Stock Ledger Entry`.`posting_datetime` ASC,
        `tabWMS Stock Ledger Entry`.`name` ASC
        LIMIT {limit}
    """
    return frappe.db.sql(sql, params, as_dict=True)