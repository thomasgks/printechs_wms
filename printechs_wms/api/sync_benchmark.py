# -*- coding: utf-8 -*-
"""Record WMS stock sync performance + data snapshots for before/after comparison."""
from __future__ import annotations

import json
import time
from datetime import datetime

import frappe
from frappe.utils import cint, flt, get_site_path

COMPANY = "Mohammed Abdullah Almousa Trading Company"
SAMPLE_ITEM = "320125"
WAREHOUSE = "Main Warehouse - MAATC"
WAREHOUSE_CODE = "WH-MAIN"
STOCK_BAL_DT = "WMS Stock Balance"


def _benchmark_dir() -> Path:
    from pathlib import Path

    p = Path(get_site_path("private", "wms_sync_benchmark"))
    p.mkdir(parents=True, exist_ok=True)
    return p


def _explain_sync_query(company: str) -> dict:
    rows = frappe.db.sql(
        """
        EXPLAIN SELECT name, company, warehouse, item_code, location, carton, qty,
               reserved_qty, last_txn_datetime, modified
        FROM `tabWMS Stock Balance`
        WHERE company=%s AND qty > 0
        ORDER BY last_txn_datetime asc, modified asc
        LIMIT 500
        """,
        (company,),
        as_dict=True,
    )
    return rows[0] if rows else {}


def _time_call(method: str, kwargs: dict) -> dict:
    start = time.perf_counter()
    out = frappe.call(method, **kwargs)
    elapsed_ms = round((time.perf_counter() - start) * 1000, 1)
    rows = out.get("rows") or []
    return {
        "elapsed_ms": elapsed_ms,
        "count": out.get("count") or len(rows),
        "has_more": out.get("has_more"),
        "total_balance_qty": out.get("total_balance_qty"),
    }


def _item_snapshot(item_code: str) -> dict:
    rows = frappe.get_all(
        STOCK_BAL_DT,
        filters={"item_code": item_code, "qty": [">", 0]},
        fields=["location", "carton", "qty", "reserved_qty", "last_txn_datetime"],
        order_by="location asc, carton asc",
        limit_page_length=5000,
    )
    total = sum(flt(r.get("qty")) for r in rows)
    return {
        "item_code": item_code,
        "row_count": len(rows),
        "total_qty": total,
        "rows": rows,
    }


@frappe.whitelist()
def record_wms_sync_benchmark(phase: str = "manual", note: str = ""):
    """Write a JSON benchmark file under sites/<site>/private/wms_sync_benchmark/."""
    phase = (phase or "manual").strip().lower()
    company = COMPANY

    stats = frappe.db.sql(
        """
        SELECT COUNT(*) AS total_rows,
               SUM(qty > 0) AS qty_gt_zero
        FROM `tabWMS Stock Balance`
        """,
        as_dict=True,
    )[0]

    indexes = frappe.db.sql("SHOW INDEX FROM `tabWMS Stock Balance`", as_dict=True)

    payload = {
        "recorded_at": datetime.utcnow().isoformat() + "Z",
        "site": frappe.local.site,
        "phase": phase,
        "note": note,
        "table_stats": stats,
        "indexes": [
            {
                "Key_name": r.get("Key_name"),
                "Column_name": r.get("Column_name"),
                "Seq_in_index": r.get("Seq_in_index"),
            }
            for r in indexes
        ],
        "explain_sync_page": _explain_sync_query(company),
        "api_timings": {
            "pull_page_default": _time_call(
                "printechs_wms.api.stock_balance.pull_stock_balances_for_wms",
                {"company": company},
            ),
            "item_totals": _time_call(
                "printechs_wms.api.stock_balance.get_item_stock_totals",
                {"item_code": SAMPLE_ITEM, "company": company},
            ),
            "item_breakdown_wh_code": _time_call(
                "printechs_wms.api.stock_balance.get_item_location_carton_balance",
                {
                    "item_code": SAMPLE_ITEM,
                    "company": company,
                    "warehouse": WAREHOUSE_CODE,
                },
            ),
        },
        "sample_item_snapshot": _item_snapshot(SAMPLE_ITEM),
        "estimated_full_sync_pages_500": int((stats.get("qty_gt_zero") or 0) // 500) + 1,
        "estimated_full_sync_pages_5000": int((stats.get("qty_gt_zero") or 0) // 5000) + 1,
    }

    out_dir = _benchmark_dir()
    fname = f"{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{phase}.json"
    out_path = out_dir / fname
    out_path.write_text(json.dumps(payload, indent=2, default=str))
    payload["benchmark_file"] = str(out_path)
    return payload
