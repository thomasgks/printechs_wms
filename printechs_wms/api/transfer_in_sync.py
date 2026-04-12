# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import frappe
from frappe import _
from frappe.utils import getdate


API_VERSION = "1.2.1"


# -----------------------------
# Helpers
# -----------------------------
def _get_arg(key: str, default=None):
    """Read argument from frappe.form_dict first, then request.args, else default."""
    try:
        v = None
        if hasattr(frappe, "form_dict") and frappe.form_dict:
            v = frappe.form_dict.get(key)
        if v in (None, ""):
            try:
                v = frappe.request.args.get(key)  # type: ignore
            except Exception:
                pass
        return default if v in (None, "") else v
    except Exception:
        return default


def _as_int(v, default=0) -> int:
    try:
        if v is None:
            return int(default)
        if isinstance(v, bool):
            return 1 if v else 0
        s = str(v).strip().lower()
        if s in ("true", "yes", "y", "1"):
            return 1
        if s in ("false", "no", "n", "0"):
            return 0
        return int(float(s))
    except Exception:
        return int(default)


def _as_float(v, default=0.0) -> float:
    try:
        return float(v or 0)
    except Exception:
        return float(default)


def _parse_docstatus(v):
    """
    Accepts: 0/1/"all"/None
    Returns: int 0/1 or None (means don't filter)
    """
    if v is None:
        return 1  # keep your old default as Submitted
    if isinstance(v, int):
        return v
    s = str(v).strip().lower()
    if s in ("all", "*"):
        return None
    if s in ("0", "draft"):
        return 0
    if s in ("1", "submitted", "submit"):
        return 1
    try:
        return int(s)
    except Exception:
        return 1


def _safe_request_args():
    try:
        return dict(getattr(frappe.request, "args", {}) or {})  # type: ignore
    except Exception:
        return {}


def _safe_json_body():
    try:
        data = getattr(frappe.request, "data", None)  # type: ignore
        if not data:
            return None
        if isinstance(data, (bytes, bytearray)):
            data = data.decode("utf-8", "ignore")
        data = (data or "").strip()
        if not data:
            return None
        return json.loads(data)
    except Exception:
        return None


def _row_to_dict(row):
    try:
        return dict(row)
    except Exception:
        return row


def _warehouse_code(warehouse_name: str | None):
    """
    Returns Warehouse.code if exists, else None.
    """
    if not warehouse_name:
        return None
    try:
        return frappe.db.get_value("Warehouse", warehouse_name, "code")
    except Exception:
        return None


def _date_filter(filters: dict, from_date: str | None, to_date: str | None):
    """
    Apply posting_date range filter.
    Accepts YYYY-MM-DD strings.
    """
    if from_date and to_date:
        filters["posting_date"] = ["between", [getdate(from_date), getdate(to_date)]]
    elif from_date:
        filters["posting_date"] = [">=", getdate(from_date)]
    elif to_date:
        filters["posting_date"] = ["<=", getdate(to_date)]


# -----------------------------
# API
# -----------------------------
@frappe.whitelist(methods=["GET"])
def get_material_transfer_stock_entries(
    to_warehouse: str | None = None,
    docstatus: int | str | None = 1,
    page: int = 1,
    page_size: int = 20,
    from_date: str | None = None,   # "YYYY-MM-DD"
    to_date: str | None = None,     # "YYYY-MM-DD"
    debug: int | str | None = 0,

    # NEW params
    add_to_transit: int | str | None = 0,               # 1/0/true/false
    custom_receiving_warehouse: str | None = None,      # ERPNext Warehouse name
):
    """
    Desktop Transfer In module can call this API to get ERPNext Stock Entries.

    MODE A (normal):
      - add_to_transit = 0
      - REQUIRED: to_warehouse
      - Filters:
          stock_entry_type="Material Transfer"
          to_warehouse=<to_warehouse>

    MODE B (transit-mode):
      - add_to_transit = 1
      - REQUIRED: custom_receiving_warehouse
      - Filters:
          stock_entry_type="Material Transfer"
          add_to_transit=1
          custom_receiving_warehouse=<custom_receiving_warehouse>
        + (optional) to_warehouse if you pass it (eg: Goods In Transit - MAATC)

    IMPORTANT UI mapping:
      - From Showroom MUST come from Stock Entry header "from_warehouse"
        so response includes:
          from_showroom = from_warehouse (alias)
          from_showroom_code = warehouse.code (alias)
    """

    # -----------------------------
    # Read missing args from request
    # -----------------------------
    if not to_warehouse:
        to_warehouse = _get_arg("to_warehouse")

    if not custom_receiving_warehouse:
        custom_receiving_warehouse = _get_arg("custom_receiving_warehouse")

    if not from_date:
        from_date = _get_arg("from_date")

    if not to_date:
        to_date = _get_arg("to_date")

    docstatus = _get_arg("docstatus", docstatus)
    docstatus = _parse_docstatus(docstatus)   # int or None

    page = _as_int(_get_arg("page", page), page)
    page_size = _as_int(_get_arg("page_size", page_size), page_size)
    debug = _as_int(_get_arg("debug", debug), 0)

    add_to_transit = _as_int(_get_arg("add_to_transit", add_to_transit), 0)

    if debug:
        frappe.log_error(
            "DEBUG transfer_in_sync args",
            f"form_dict={dict(getattr(frappe, 'form_dict', {}) or {})} | "
            f"request.args={_safe_request_args()} | json={_safe_json_body()}"
        )

    page = max(page, 1)
    page_size = min(max(page_size, 1), 200)

    # -----------------------------
    # Build filters
    # -----------------------------
    filters = {
        "stock_entry_type": "Material Transfer",
    }

    # docstatus filter (None => don't filter)
    if docstatus is not None:
        filters["docstatus"] = int(docstatus)

    # MODE B: Transit-mode
    if add_to_transit == 1:
        if not custom_receiving_warehouse or not str(custom_receiving_warehouse).strip():
            frappe.throw(
                _("custom_receiving_warehouse is required when add_to_transit=1. "
                  "Example: ?add_to_transit=1&custom_receiving_warehouse=Main%20Warehouse%20-%20MAATC")
            )

        custom_receiving_warehouse = str(custom_receiving_warehouse).strip()

        # These fields MUST exist in your Stock Entry (custom fields)
        # - add_to_transit (Check / Int)
        # - custom_receiving_warehouse (Link to Warehouse)
        filters["add_to_transit"] = 1
        filters["custom_receiving_warehouse"] = custom_receiving_warehouse

        # optional to_warehouse (usually transit warehouse)
        if to_warehouse and str(to_warehouse).strip():
            filters["to_warehouse"] = str(to_warehouse).strip()

    # MODE A: Normal mode (old behavior)
    else:
        if not to_warehouse or not str(to_warehouse).strip():
            frappe.throw(
                _("to_warehouse is required. Example: ?to_warehouse=Main%20Warehouse%20-%20MAATC")
            )
        filters["to_warehouse"] = str(to_warehouse).strip()

    # Optional posting_date filters
    _date_filter(filters, from_date, to_date)

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
        # include if exists
        "add_to_transit",
        "custom_receiving_warehouse",
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

        from_wh_code = _warehouse_code(from_wh)
        to_wh_code = _warehouse_code(to_wh)

        header = {
            "api_version": API_VERSION,

            "stock_entry_no": doc.name,
            "stock_entry_type": doc.stock_entry_type,
            "company": doc.company,
            "posting_date": str(doc.posting_date),
            "posting_time": str(doc.posting_time),
            "docstatus": doc.docstatus,

            # ERPNext true fields
            "from_warehouse": from_wh,
            "from_warehouse_code": from_wh_code,
            "to_warehouse": to_wh,
            "to_warehouse_code": to_wh_code,

            # ✅ Desktop UI aliases (From Showroom)
            "from_showroom": from_wh,                  # <-- bind this if UI column says From Showroom
            "from_showroom_code": from_wh_code,        # <-- bind this if UI wants code (001-Unaizah / INTRANS)

            # Optional Transit fields (safe)
            "add_to_transit": _as_int(getattr(doc, "add_to_transit", 0), 0),
            "custom_receiving_warehouse": getattr(doc, "custom_receiving_warehouse", None),

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

                "basic_rate": _as_float(getattr(it, "basic_rate", 0)),
                "basic_amount": _as_float(getattr(it, "basic_amount", 0)),
                "serial_no": getattr(it, "serial_no", None),
                "batch_no": getattr(it, "batch_no", None),
            })

        results.append(header)

    return {
        "ok": True,
        "api_version": API_VERSION,
        "mode": "transit" if add_to_transit == 1 else "normal",
        "filters_used": {
            "stock_entry_type": "Material Transfer",
            "docstatus": docstatus if docstatus is not None else "all",
            "add_to_transit": add_to_transit,
            "to_warehouse": (str(to_warehouse).strip() if to_warehouse else None),
            "custom_receiving_warehouse": (str(custom_receiving_warehouse).strip() if custom_receiving_warehouse else None),
            "from_date": from_date,
            "to_date": to_date,
            "page": page,
            "page_size": page_size,
        },
        "count": len(results),
        "total_count": total_count,
        "data": results,
    }