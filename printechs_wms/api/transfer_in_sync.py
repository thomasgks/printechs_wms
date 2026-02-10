from __future__ import annotations

import frappe
from frappe import _

API_VERSION = "transfer_in_from_stock_entry_v1"


# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------

def _as_int(v, default=0) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _as_float(v, default=0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _row_to_dict(row) -> dict:
    """Frappe may return dict or object depending on call."""
    if isinstance(row, dict):
        return row
    try:
        return row.as_dict()
    except Exception:
        return dict(row)


def _warehouse_code(warehouse_name: str | None) -> str | None:
    """
    Return Warehouse.code by Warehouse.name (Link value).
    No need for extra link field.
    """
    if not warehouse_name:
        return None
    return frappe.db.get_value("Warehouse", warehouse_name, "code")


def _get_arg(key: str, default=None):
    """
    Read from request args safely.
    Works for /api/method calls (querystring or formdata).
    """
    return frappe.form_dict.get(key, default)


# ------------------------------------------------------------
# API: List Stock Entries (Material Transfer) -> Desktop Transfer In feed
# ------------------------------------------------------------
@frappe.whitelist(methods=["GET"])
def get_material_transfer_stock_entries(
    to_warehouse: str | None = None,
    docstatus: int = 1,
    page: int = 1,
    page_size: int = 20,
    from_date: str | None = None,   # "YYYY-MM-DD"
    to_date: str | None = None,     # "YYYY-MM-DD"
):
    """
    Desktop Transfer In module can call this API to get ERPNext Stock Entries
    that represent Transfer In to warehouse (Material Transfer).

    Required:
      - to_warehouse: exact Warehouse name (e.g. "Main Warehouse - MAATC")

    Returns:
      - stock entry header + items
      - warehouse code for source/target and per line
    """

    # -----------------------------
    # Read missing args from request
    # -----------------------------
    if not to_warehouse:
        to_warehouse = _get_arg("to_warehouse")

    if not from_date:
        from_date = _get_arg("from_date")

    if not to_date:
        to_date = _get_arg("to_date")

    # numeric args can also come as string
    docstatus = _as_int(_get_arg("docstatus", docstatus), docstatus)
    page = _as_int(_get_arg("page", page), page)
    page_size = _as_int(_get_arg("page_size", page_size), page_size)

    # Validate required
    if not to_warehouse:
        frappe.throw(
            _("to_warehouse is required. Example: ?to_warehouse=Main%20Warehouse%20-%20MAATC")
        )

    page = max(page, 1)
    page_size = min(max(page_size, 1), 200)

    # -----------------------------
    # Build filters
    # -----------------------------
    filters = {
        "stock_entry_type": "Material Transfer",
        "docstatus": docstatus,
        "to_warehouse": to_warehouse,  # header default target warehouse
    }

    # Optional posting_date filters
    if from_date and to_date:
        filters["posting_date"] = ["between", [from_date, to_date]]
    elif from_date:
        filters["posting_date"] = [">=", from_date]
    elif to_date:
        filters["posting_date"] = ["<=", to_date]

    fields = [
        "name",
        "stock_entry_type",
        "company",
        "posting_date",
        "posting_time",
        "from_warehouse",
        "to_warehouse",
        "remarks",
        "docstatus",
        "owner",
        "creation",
        "modified",
    ]

    offset = (page - 1) * page_size

    # -----------------------------
    # Query headers
    # -----------------------------
    stock_entries = frappe.get_all(
        "Stock Entry",
        filters=filters,
        fields=fields,
        order_by="posting_date desc, modified desc",
        start=offset,
        page_length=page_size,
    )

    total_count = frappe.db.count("Stock Entry", filters=filters)

    # -----------------------------
    # Build response
    # -----------------------------
    results = []

    for se in stock_entries:
        se = _row_to_dict(se)
        doc = frappe.get_doc("Stock Entry", se["name"])

        from_wh = doc.from_warehouse
        to_wh = doc.to_warehouse

        header = {
            "api_version": API_VERSION,
            "stock_entry_no": doc.name,
            "stock_entry_type": doc.stock_entry_type,
            "company": doc.company,
            "posting_date": str(doc.posting_date),
            "posting_time": str(doc.posting_time),
            "docstatus": doc.docstatus,

            "from_warehouse": from_wh,
            "from_warehouse_code": _warehouse_code(from_wh),

            "to_warehouse": to_wh,
            "to_warehouse_code": _warehouse_code(to_wh),

            "remarks": doc.remarks,
            "owner": doc.owner,
            "creation": str(doc.creation),
            "modified": str(doc.modified),

            "items": [],
        }

        for it in doc.items:
            # Prefer line warehouses; fallback to header
            s_wh = it.s_warehouse or from_wh
            t_wh = it.t_warehouse or to_wh

            header["items"].append({
                "idx": it.idx,
                "item_code": it.item_code,
                "item_name": it.item_name,
                "qty": _as_float(it.qty),
                "uom": it.uom,

                "s_warehouse": s_wh,
                "s_warehouse_code": _warehouse_code(s_wh),

                "t_warehouse": t_wh,
                "t_warehouse_code": _warehouse_code(t_wh),

                # optional useful fields
                "basic_rate": _as_float(it.basic_rate),
                "basic_amount": _as_float(it.basic_amount),
                "serial_no": it.serial_no,
                "batch_no": it.batch_no,
            })

        results.append(header)

    return {
        "ok": True,
        "api_version": API_VERSION,
        "filters_used": {
            "stock_entry_type": "Material Transfer",
            "docstatus": docstatus,
            "to_warehouse": to_warehouse,
            "from_date": from_date,
            "to_date": to_date,
            "page": page,
            "page_size": page_size,
        },
        "count": len(results),
        "total_count": total_count,
        "data": results,
    }
