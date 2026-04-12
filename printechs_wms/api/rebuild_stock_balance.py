# -*- coding: utf-8 -*-
from __future__ import annotations
import frappe


def _pick_first(cols: set[str], candidates: list[str]) -> str | None:
    for c in candidates:
        if c in cols:
            return c
    return None


@frappe.whitelist()
def rebuild_wms_stock_balance(company: str, warehouse: str | None = None):
    """
    Rebuild WMS Stock Balance from WMS Stock Ledger Entry.
    - Auto-detect ledger column names
    - Handles mandatory warehouse in WMS Stock Balance
    """
    if not company:
        frappe.throw("company is required")

    LEDGER = "WMS Stock Ledger Entry"
    BAL = "WMS Stock Balance"

    if not frappe.db.exists("DocType", LEDGER):
        frappe.throw(f"Missing DocType: {LEDGER}")
    if not frappe.db.exists("DocType", BAL):
        frappe.throw(f"Missing DocType: {BAL}")

    # Detect DB columns
    ledger_cols = set(frappe.db.get_table_columns(LEDGER))
    bal_cols = set(frappe.db.get_table_columns(BAL))

    # Ledger: find location/carton/qty_after/datetime
    location_col = _pick_first(ledger_cols, ["location", "bin_location", "bin", "bin_code", "source_location", "target_location"])
    carton_col = _pick_first(ledger_cols, ["carton", "carton_id", "carton_no"])
    qty_after_col = _pick_first(ledger_cols, ["qty_after", "qty_balance", "balance_qty", "qty"])
    posting_dt_col = _pick_first(ledger_cols, ["posting_datetime", "transaction_date", "posting_date", "creation", "modified"])

    # Ledger: try to find warehouse field (optional)
    ledger_wh_col = _pick_first(ledger_cols, ["warehouse", "warehouse_code", "wms_warehouse", "erp_warehouse"])

    # Balance: mandatory warehouse column exists?
    bal_requires_warehouse = "warehouse" in bal_cols

    if not location_col or not carton_col or not qty_after_col or not posting_dt_col:
        frappe.throw(
            "Cannot rebuild: required ledger columns not found.\n"
            f"Found ledger columns: {sorted(list(ledger_cols))}"
        )

    # If balance requires warehouse but ledger doesn't have it, warehouse param must be provided
    if bal_requires_warehouse and not ledger_wh_col and not warehouse:
        frappe.throw(
            "WMS Stock Balance requires 'warehouse' but WMS Stock Ledger Entry has no warehouse column. "
            "Pass 'warehouse' in API body."
        )

    # Clear balances (optionally per warehouse)
    if bal_requires_warehouse and warehouse:
        frappe.db.sql(f"DELETE FROM `tab{BAL}` WHERE company=%s AND warehouse=%s", (company, warehouse))
    else:
        frappe.db.sql(f"DELETE FROM `tab{BAL}` WHERE company=%s", (company,))

    # Build SQL select
    select_wh = f", `{ledger_wh_col}` AS warehouse" if ledger_wh_col else ""
    group_wh = f", `{ledger_wh_col}`" if ledger_wh_col else ""

    rows = frappe.db.sql(f"""
        SELECT
            item_code,
            `{location_col}` AS location,
            `{carton_col}` AS carton
            {select_wh},
            SUBSTRING_INDEX(
                GROUP_CONCAT(`{qty_after_col}` ORDER BY `{posting_dt_col}` DESC),
                ',', 1
            ) AS qty_after,
            SUBSTRING_INDEX(
                GROUP_CONCAT(`{posting_dt_col}` ORDER BY `{posting_dt_col}` DESC),
                ',', 1
            ) AS last_dt
        FROM `tab{LEDGER}`
        WHERE company=%s
          AND item_code IS NOT NULL
          AND `{location_col}` IS NOT NULL
          AND `{carton_col}` IS NOT NULL
        GROUP BY item_code, `{location_col}`, `{carton_col}` {group_wh}
    """, (company,), as_dict=True)

    created = 0
    skipped_zero = 0

    for r in rows:
        qty = float(r.get("qty_after") or 0)
        if qty <= 0:
            skipped_zero += 1
            continue

        doc_dict = {
            "doctype": BAL,
            "company": company,
            "item_code": r.get("item_code"),
            "location": r.get("location"),
            "carton": r.get("carton"),
            "qty": qty,
            "reserved_qty": 0,
            "last_txn_datetime": r.get("last_dt"),
        }

        # Set warehouse (mandatory)
        if bal_requires_warehouse:
            doc_dict["warehouse"] = r.get("warehouse") or warehouse

        doc = frappe.get_doc(doc_dict)
        doc.insert(ignore_permissions=True)
        created += 1

    frappe.db.commit()

    return {
        "ok": True,
        "company": company,
        "warehouse_used": warehouse or ("from_ledger" if ledger_wh_col else None),
        "rows_created": created,
        "rows_scanned": len(rows),
        "skipped_zero": skipped_zero,
        "mapping": {
            "ledger_location_col": location_col,
            "ledger_carton_col": carton_col,
            "ledger_qty_after_col": qty_after_col,
            "ledger_posting_dt_col": posting_dt_col,
            "ledger_warehouse_col": ledger_wh_col,
        }
    }
