# -*- coding: utf-8 -*-
"""
Desktop compatibility API for WMS stock balance pull + location breakdown.

The Printechs WMS desktop app calls printechs_wms.api.stock_balance.* — this module
forwards to the canonical implementations in wms_stock_drilldown / cycle_count_batch.
"""
from __future__ import annotations

import frappe
from frappe import _
from frappe.utils import cint, flt


@frappe.whitelist()
def get_item_location_carton_balance(item_code=None, warehouse=None, company=None, limit=500, include_zero=0):
    """
    Location breakdown for desktop Item screen (Show Location Breakdown).
    Maps drilldown rows to desktop column names.
    """
    from printechs_wms.api.wms_stock_drilldown import get_item_location_carton_balance as _fetch

    resp = _fetch(
        item_code=item_code,
        warehouse=warehouse,
        company=company,
        limit=limit,
    )
    if not resp.get("ok"):
        return resp

    include_zero = cint(include_zero or 0)
    rows = []
    for r in resp.get("rows") or []:
        balance = flt(r.get("balance"))
        if not include_zero and balance == 0:
            continue
        rows.append(
            {
                "location_id": r.get("location"),
                "carton_id": (r.get("carton") or "").strip(),
                "warehouse": r.get("warehouse"),
                "in_qty": 0,
                "out_qty": 0,
                "balance_qty": balance,
                "reserved_qty": flt(r.get("reserved")),
                "available_qty": flt(r.get("available")),
                "last_txn_datetime": r.get("last_txn_datetime"),
            }
        )

    total = sum(flt(x.get("balance_qty")) for x in rows)
    return {
        "ok": True,
        "item_code": (item_code or "").strip(),
        "warehouse": warehouse,
        "company": company,
        "rows": rows,
        "count": len(rows),
        "total_balance_qty": total,
    }


@frappe.whitelist()
def pull_stock_balances_for_wms(
    company=None,
    warehouse=None,
    item_code=None,
    last_txn_after=None,
    include_zero=0,
    limit=500,
    offset=0,
):
    """
    Incremental stock pull for desktop Sync (after cycle count post or periodically).
    """
    from printechs_wms.api.cycle_count_batch import get_stock_balance_compact

    return get_stock_balance_compact(
        company=company,
        warehouse=warehouse,
        item_code=item_code,
        last_txn_after=last_txn_after,
        include_zero=include_zero,
        limit=limit,
        offset=offset,
    )


@frappe.whitelist()
def get_stock_balance_compact(
    company=None,
    warehouse=None,
    item_code=None,
    last_txn_after=None,
    include_zero=0,
    limit=500,
    offset=0,
):
    """Alias used by some desktop builds."""
    return pull_stock_balances_for_wms(
        company=company,
        warehouse=warehouse,
        item_code=item_code,
        last_txn_after=last_txn_after,
        include_zero=include_zero,
        limit=limit,
        offset=offset,
    )


@frappe.whitelist()
def get_item_stock_totals(item_code=None, warehouse=None, company=None, include_zero=0):
    """Item-level total + breakdown for desktop."""
    from printechs_wms.api.cycle_count_batch import get_item_wms_stock_for_desktop

    return get_item_wms_stock_for_desktop(
        item_code=item_code,
        warehouse=warehouse,
        company=company,
        include_zero=include_zero,
    )
