# -*- coding: utf-8 -*-
from __future__ import annotations

import frappe
from frappe.utils import cint, flt, cstr


def execute(filters=None):
    filters = filters or {}
    columns = get_columns()
    data = get_data(filters)
    return columns, data


def get_columns():
    return [
        {"label": "Company", "fieldname": "company", "fieldtype": "Link", "options": "Company", "width": 160},
        {"label": "Warehouse", "fieldname": "warehouse", "fieldtype": "Link", "options": "Warehouse", "width": 220},
        {"label": "Item", "fieldname": "item_code", "fieldtype": "Link", "options": "Item", "width": 140},
        {"label": "Item Name", "fieldname": "item_name", "fieldtype": "Data", "width": 220},
        {"label": "Location", "fieldname": "location", "fieldtype": "Data", "width": 140},
        {"label": "Carton", "fieldname": "carton", "fieldtype": "Data", "width": 140},
        {"label": "Qty", "fieldname": "qty", "fieldtype": "Float", "width": 110},
        {"label": "Reserved Qty", "fieldname": "reserved_qty", "fieldtype": "Float", "width": 120},
        {"label": "Available Qty", "fieldname": "available_qty", "fieldtype": "Float", "width": 120},
        {"label": "Last Txn", "fieldname": "last_txn_datetime", "fieldtype": "Datetime", "width": 170},
        {"label": "Doc", "fieldname": "name", "fieldtype": "Link", "options": "WMS Stock Balance", "width": 170},
    ]


def get_data(filters):
    company = cstr(filters.get("company") or "").strip()
    if not company:
        frappe.throw("Company is required")

    warehouse = cstr(filters.get("warehouse") or "").strip()
    item_code = cstr(filters.get("item_code") or "").strip()
    location = cstr(filters.get("location") or "").strip()
    carton = cstr(filters.get("carton") or "").strip()

    only_positive_qty = cint(filters.get("only_positive_qty") or 0)
    show_zero_qty = cint(filters.get("show_zero_qty") or 0)
    limit = cint(filters.get("limit") or 500)

    conditions = ["sb.company = %(company)s"]
    params = {"company": company, "limit": limit}

    if warehouse:
        conditions.append("sb.warehouse = %(warehouse)s")
        params["warehouse"] = warehouse

    if item_code:
        conditions.append("sb.item_code = %(item_code)s")
        params["item_code"] = item_code

    if location:
        conditions.append("sb.location = %(location)s")
        params["location"] = location

    if carton:
        conditions.append("sb.carton = %(carton)s")
        params["carton"] = carton

    if only_positive_qty:
        conditions.append("IFNULL(sb.qty, 0) > 0")
    elif not show_zero_qty:
        conditions.append("IFNULL(sb.qty, 0) != 0")

    where_sql = " AND ".join(conditions)

    return frappe.db.sql(
        f"""
        SELECT
            sb.name,
            sb.company,
            sb.warehouse,
            sb.item_code,
            i.item_name,
            sb.location,
            sb.carton,
            sb.qty,
            sb.reserved_qty,
            (IFNULL(sb.qty,0) - IFNULL(sb.reserved_qty,0)) AS available_qty,
            sb.last_txn_datetime
        FROM `tabWMS Stock Balance` sb
        LEFT JOIN `tabItem` i ON i.name = sb.item_code
        WHERE {where_sql}
        ORDER BY sb.modified DESC
        LIMIT %(limit)s
        """,
        params,
        as_dict=True
    )