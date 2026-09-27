# -*- coding: utf-8 -*-
from __future__ import annotations

import frappe


def normalize_stock_balance_warehouse(warehouse: str | None) -> str | None:
    from frappe.utils import cstr
    from printechs_wms.api.warehouse import resolve_warehouse_docname

    warehouse = cstr(warehouse).strip() if warehouse else None
    if not warehouse:
        return None
    resolved = resolve_warehouse_docname(name=warehouse, code=warehouse, warehouse_name=warehouse)
    return resolved or warehouse

STOCK_BAL_TABLE = "`tabWMS Stock Balance`"

INDEX_DEFINITIONS = [
    ("wms_stock_bal_company_item_wh", ["company", "item_code", "warehouse"]),
    ("wms_stock_bal_company_wh_item", ["company", "warehouse", "item_code"]),
    ("wms_stock_bal_company_qty_txn", ["company", "qty", "last_txn_datetime", "modified"]),
]


def _existing_index_names() -> set[str]:
    rows = frappe.db.sql(f"SHOW INDEX FROM {STOCK_BAL_TABLE}", as_dict=True)
    return {r.get("Key_name") for r in rows if r.get("Key_name")}


def ensure_wms_stock_balance_indexes(dry_run: int = 0) -> dict:
    existing = _existing_index_names()
    created, skipped = [], []

    for name, columns in INDEX_DEFINITIONS:
        if name in existing:
            skipped.append(name)
            continue
        cols_sql = ", ".join(f"`{c}`" for c in columns)
        sql = f"ALTER TABLE {STOCK_BAL_TABLE} ADD INDEX `{name}` ({cols_sql})"
        if dry_run:
            created.append({"index": name, "sql": sql, "dry_run": True})
            continue
        frappe.db.sql(sql)
        created.append(name)

    if not dry_run and created:
        frappe.db.commit()

    return {
        "ok": True,
        "dry_run": bool(int(dry_run or 0)),
        "created": created,
        "skipped": skipped,
        "existing_before": sorted(existing),
    }


@frappe.whitelist()
def add_wms_stock_balance_indexes(dry_run: int = 0):
    return ensure_wms_stock_balance_indexes(dry_run=dry_run)
