# -*- coding: utf-8 -*-
from __future__ import annotations

import frappe
from frappe.utils import getdate


@frappe.whitelist()
def stock_check_by_item(
    barcode=None,
    item_code=None,
    style=None,
    company=None,
    include_zero=1,
    payload=None,
    **kwargs,
):
    """
    POS stock by warehouse (for POS screen):
        pos_qty = Bin.actual_qty - pending_pos_qty

    pending_pos_qty comes from:
        `tabPOS Invoice Item`
        JOIN `tabPOS Invoice`
        WHERE `tabPOS Invoice`.consolidated_invoice IS NULL

    Supports:
    - item_code OR barcode  -> single-item mode
    - style (variant_of)    -> returns ALL variant items under that style

    Accepts:
    - body: {"style":"1395", ...}
    - nested: {"payload": {"style":"1395", ...}}
    - kwarg: payload=<dict>

    CONDITIONS:
    1) custom_exclude = 0  (if field exists on POS Invoice or POS Invoice Item)
    2) posting_date >= last Stock Reconciliation date per item+warehouse (dynamic)
       Falls back to 2026-01-01 if no reconciliation exists.
       Can be overridden by request key: posting_date_from / from_date / from_posting_date
       (override applies globally to all items/warehouses in the request)

    FIX: pending_pos_qty now only counts POS Invoices posted AFTER the last
    Stock Reconciliation for each item+warehouse combination. This prevents
    double-deduction when a reconciliation has already absorbed prior POS sales
    into the Bin's actual_qty.
    """

    # -----------------------------
    # Robust input handling (GET/POST)
    # -----------------------------
    req = getattr(frappe.local, "request", None)

    args = {}
    if req:
        try:
            args = dict(req.args)
        except Exception:
            args = {}

    body = {}
    if req:
        try:
            body = req.get_json(silent=True) or {}
        except Exception:
            body = {}

    # Merge payload into body (fix payload keyword + nested payload)
    if payload and isinstance(payload, dict):
        try:
            body = {**body, **payload}
        except Exception:
            pass

    if isinstance(body, dict) and isinstance(body.get("payload"), dict):
        try:
            body = {**body, **body.get("payload")}
        except Exception:
            pass

    fd = frappe.form_dict or {}

    def pick(*keys):
        for k in keys:
            if k in fd and fd.get(k) not in (None, ""):
                return fd.get(k)
            if k in args and args.get(k) not in (None, ""):
                return args.get(k)
            if k in body and body.get(k) not in (None, ""):
                return body.get(k)
        return None

    barcode    = (barcode    or pick("barcode", "bar_code", "scan") or "").strip()
    item_code  = (item_code  or pick("item_code", "item", "code")   or "").strip()
    style      = (style      or pick("style", "variant_of")         or "").strip()
    company    = (company    or pick("company")                      or "").strip()

    inc0 = pick("include_zero")
    include_zero = int(inc0 or 0) if inc0 is not None else int(include_zero or 0)

    # custom_exclude filter: only count invoices where custom_exclude = 0
    ce = pick("custom_exclude")
    custom_exclude = int(ce) if ce not in (None, "", "null") else 0

    # Manual override: caller can pin a single global posting_date_from
    fd_from = pick("posting_date_from", "from_date", "from_posting_date")
    manual_posting_date_from = getdate(fd_from) if fd_from not in (None, "", "null") else None

    if not company:
        company = frappe.defaults.get_user_default("Company")

    if not item_code and barcode:
        item_code = _item_code_from_barcode(barcode)

    if not item_code and not style:
        frappe.throw(
            f"Provide item_code or barcode or style. "
            f"Received barcode='{barcode}' item_code='{item_code}' style='{style}'"
        )

    # -----------------------------
    # Determine items to process
    # -----------------------------
    mode = "item"
    item_codes: list[str] = []

    if item_code:
        mode = "item"
        item_codes = [item_code]
        if not style and frappe.db.has_column("Item", "variant_of"):
            style = frappe.db.get_value("Item", item_code, "variant_of") or ""
    else:
        # STYLE MODE: Exact first, then LIKE wildcard fallback
        mode = "style"
        if not frappe.db.has_column("Item", "variant_of"):
            frappe.throw("Item.variant_of field not found. Cannot run style mode.")

        # 1) Exact match (fast)
        item_codes = frappe.get_all("Item", filters={"variant_of": style}, pluck="name") or []

        # 2) Wildcard fallback (flexible)
        if not item_codes:
            item_codes = frappe.get_all(
                "Item",
                filters={"variant_of": ["like", f"%{style}%"]},
                pluck="name",
            ) or []

        if not item_codes:
            frappe.throw(f"No variant items found matching style '{style}' (exact or LIKE).")

    # -----------------------------
    # Load Color/Size for items
    # -----------------------------
    attr_map = _get_color_size_map(item_codes, style_hint=style)

    # -----------------------------
    # 1) ERP stock from Bin
    #    bin_map[item_code][warehouse] = actual_qty
    # -----------------------------
    bin_map: dict[str, dict[str, float]] = {ic: {} for ic in item_codes}
    bins = frappe.get_all(
        "Bin",
        filters={"item_code": ["in", item_codes]},
        fields=["item_code", "warehouse", "actual_qty"],
        order_by="item_code asc, warehouse asc",
    )
    for b in bins:
        ic = b["item_code"]
        wh = b["warehouse"]
        bin_map.setdefault(ic, {})
        bin_map[ic][wh] = float(b.get("actual_qty") or 0)

    # -----------------------------
    # 2) Last Stock Reconciliation date per item+warehouse
    #    recon_date_map[(item_code, warehouse)] = date
    #
    #    This is the core fix: we only count POS Invoices posted AFTER the
    #    last reconciliation, because the reconciliation already baked prior
    #    POS sales into Bin.actual_qty.  If the caller supplied a manual
    #    posting_date_from override we skip this lookup and use that instead.
    # -----------------------------
    recon_date_map: dict[tuple[str, str], object] = {}

    if not manual_posting_date_from:
        in_placeholders_recon = ", ".join(["%s"] * len(item_codes))
        recon_rows = frappe.db.sql(
            f"""
            SELECT
                item_code,
                warehouse,
                MAX(posting_date) AS last_recon_date
            FROM `tabStock Ledger Entry`
            WHERE
                item_code IN ({in_placeholders_recon})
                AND voucher_type = 'Stock Reconciliation'
                AND docstatus = 1
            GROUP BY item_code, warehouse
            """,
            item_codes,
            as_dict=True,
        )
        for r in recon_rows:
            if r.get("last_recon_date"):
                recon_date_map[(r["item_code"], r["warehouse"])] = getdate(r["last_recon_date"])

    # Fallback date used when no reconciliation exists for an item+warehouse
    FALLBACK_DATE = getdate("2026-01-01")

    # -----------------------------
    # 3) Pending POS qty (NOT consolidated), filtered per item+warehouse
    #    pending_map[item_code][warehouse] = pending_qty
    #
    #    Because the cutoff date can differ per item+warehouse we build the
    #    query dynamically with CASE WHEN per-row date filtering.
    #    If a manual override was supplied we use a single date for all rows.
    # -----------------------------
    POS_INV  = "POS Invoice"
    POS_ITEM = "POS Invoice Item"

    if not frappe.db.exists("DocType", POS_INV) or not frappe.db.exists("DocType", POS_ITEM):
        frappe.throw("POS DocTypes not found: POS Invoice / POS Invoice Item")

    inv_cols  = set(frappe.db.get_table_columns(POS_INV)  or [])
    item_cols = set(frappe.db.get_table_columns(POS_ITEM) or [])

    if "warehouse" in item_cols:
        wh_expr = "pii.warehouse"
    elif "set_warehouse" in inv_cols:
        wh_expr = "pi.set_warehouse"
    else:
        frappe.throw(
            "Need POS Invoice Item.warehouse OR POS Invoice.set_warehouse to group by warehouse."
        )

    qty_field      = "pii.stock_qty" if "stock_qty" in item_cols else "pii.qty"
    qty_field_used = "stock_qty"     if "stock_qty" in item_cols else "qty"

    # Build extra SQL clauses and their params
    extra_clauses: list[str] = []
    extra_params:  list      = []

    if "docstatus" in inv_cols:
        extra_clauses.append("AND pi.docstatus IN (0, 1)")

    if "company" in inv_cols and company:
        extra_clauses.append("AND pi.company = %s")
        extra_params.append(company)

    if "custom_exclude" in inv_cols:
        extra_clauses.append("AND IFNULL(pi.custom_exclude, 0) = %s")
        extra_params.append(custom_exclude)
    elif "custom_exclude" in item_cols:
        extra_clauses.append("AND IFNULL(pii.custom_exclude, 0) = %s")
        extra_params.append(custom_exclude)

    extra_sql = "\n            ".join(extra_clauses)

    in_placeholders = ", ".join(["%s"] * len(item_codes))

    pending_map: dict[str, dict[str, float]] = {ic: {} for ic in item_codes}

    if "posting_date" in inv_cols:
        if manual_posting_date_from:
            # ── Single global date override supplied by caller ──────────────
            params = list(item_codes) + [manual_posting_date_from] + extra_params
            pending = frappe.db.sql(
                f"""
                SELECT
                    pii.item_code  AS item_code,
                    {wh_expr}      AS warehouse,
                    SUM({qty_field}) AS pending_qty
                FROM `tabPOS Invoice` pi
                INNER JOIN `tabPOS Invoice Item` pii ON pii.parent = pi.name
                WHERE
                    pii.item_code IN ({in_placeholders})
                    AND pi.consolidated_invoice IS NULL
                    AND pi.posting_date >= %s
                    {extra_sql}
                    AND {wh_expr} IS NOT NULL
                GROUP BY pii.item_code, {wh_expr}
                """,
                params,
                as_dict=True,
            )
            posting_date_from_reported = str(manual_posting_date_from)

        else:
            # ── Dynamic per-item+warehouse cutoff (THE FIX) ────────────────
            #
            # Strategy: fetch ALL unconsolidated POS rows for the item list
            # first (no date filter), then filter in Python using the per-row
            # recon date. This avoids building a giant dynamic SQL CASE block
            # and works correctly even when recon dates differ per warehouse.
            params = list(item_codes) + extra_params
            raw_pending = frappe.db.sql(
                f"""
                SELECT
                    pii.item_code        AS item_code,
                    {wh_expr}            AS warehouse,
                    pi.posting_date      AS posting_date,
                    SUM({qty_field})     AS pending_qty
                FROM `tabPOS Invoice` pi
                INNER JOIN `tabPOS Invoice Item` pii ON pii.parent = pi.name
                WHERE
                    pii.item_code IN ({in_placeholders})
                    AND pi.consolidated_invoice IS NULL
                    {extra_sql}
                    AND {wh_expr} IS NOT NULL
                GROUP BY pii.item_code, {wh_expr}, pi.posting_date
                """,
                params,
                as_dict=True,
            )

            # Aggregate only rows whose posting_date is AFTER the last
            # Stock Reconciliation for that item+warehouse combination.
            pending_accum: dict[tuple[str, str], float] = {}
            for row in raw_pending:
                ic  = row["item_code"]
                wh  = row["warehouse"]
                key = (ic, wh)

                # Determine the cutoff for this specific item+warehouse
                cutoff = recon_date_map.get(key, FALLBACK_DATE)

                row_date = getdate(row["posting_date"]) if row.get("posting_date") else None
                if row_date is None or row_date <= cutoff:
                    # This POS sale was already captured by the reconciliation
                    # (or is before our fallback floor) — skip it.
                    continue

                pending_accum[key] = pending_accum.get(key, 0.0) + float(row.get("pending_qty") or 0)

            # Flatten back into the same structure used downstream
            pending = [
                {"item_code": k[0], "warehouse": k[1], "pending_qty": v}
                for k, v in pending_accum.items()
            ]

            # For reporting: show the range of cutoff dates actually applied
            if recon_date_map:
                earliest = min(str(d) for d in recon_date_map.values())
                latest   = max(str(d) for d in recon_date_map.values())
                posting_date_from_reported = (
                    earliest if earliest == latest else f"{earliest} ~ {latest} (per item+warehouse)"
                )
            else:
                posting_date_from_reported = str(FALLBACK_DATE) + " (fallback, no reconciliation found)"

    else:
        # posting_date column does not exist on POS Invoice — no date filter
        params = list(item_codes) + extra_params
        pending = frappe.db.sql(
            f"""
            SELECT
                pii.item_code  AS item_code,
                {wh_expr}      AS warehouse,
                SUM({qty_field}) AS pending_qty
            FROM `tabPOS Invoice` pi
            INNER JOIN `tabPOS Invoice Item` pii ON pii.parent = pi.name
            WHERE
                pii.item_code IN ({in_placeholders})
                AND pi.consolidated_invoice IS NULL
                {extra_sql}
                AND {wh_expr} IS NOT NULL
            GROUP BY pii.item_code, {wh_expr}
            """,
            params,
            as_dict=True,
        )
        posting_date_from_reported = "n/a (posting_date column not found)"

    for d in pending:
        ic = d["item_code"]
        wh = d["warehouse"]
        pending_map.setdefault(ic, {})
        pending_map[ic][wh] = float(d.get("pending_qty") or 0)

    # -----------------------------
    # 4) Item info
    # -----------------------------
    item_info: dict[str, dict] = {}
    items = frappe.get_all(
        "Item",
        filters={"name": ["in", item_codes]},
        fields=["name", "item_name", "stock_uom", "variant_of"],
    )
    for it in items:
        item_info[it["name"]] = it

    # -----------------------------
    # Helpers: size sorting
    # -----------------------------
    def _size_sort_value(size):
        if not size:
            return (999, "")
        s = str(size).upper().strip()

        size_order = {
            "XXXS": 1, "XXS": 2, "XS": 3, "S": 4, "M": 5,
            "L": 6,   "XL": 7, "XXL": 8, "XXXL": 9, "XXXXL": 10,
        }
        if s in size_order:
            return (0, size_order[s])

        try:
            return (1, float(s))
        except Exception:
            return (2, s)

    # -----------------------------
    # 5) Build rows grouped by warehouse
    #    stores_map[warehouse] = [rows...]
    # -----------------------------
    stores_map: dict[str, list[dict]] = {}

    for ic in item_codes:
        bins_for_item = bin_map.get(ic)     or {}
        pend_for_item = pending_map.get(ic) or {}
        warehouses = set(list(bins_for_item.keys()) + list(pend_for_item.keys()))

        it    = item_info.get(ic) or {}
        attrs = attr_map.get(ic)  or {}

        for wh in warehouses:
            erp_qty  = float(bins_for_item.get(wh) or 0)
            pend_qty = float(pend_for_item.get(wh) or 0)
            pos_qty  = erp_qty - pend_qty

            if not include_zero and abs(pos_qty) < 0.000001:
                continue

            # Surface the effective cutoff date that was applied for this row
            if manual_posting_date_from:
                effective_cutoff = str(manual_posting_date_from)
            else:
                effective_cutoff = str(recon_date_map.get((ic, wh), FALLBACK_DATE))

            row = {
                "store":              wh,
                "style":              attrs.get("style") or it.get("variant_of") or style or "",
                "item_code":          ic,
                "color":              attrs.get("color") or "",
                "size":               attrs.get("size")  or "",
                "item_name":          it.get("item_name"),
                "barcode":            barcode if mode == "item" else "",
                "erp_qty":            round(erp_qty,  3),
                "pending_pos_qty":    round(pend_qty, 3),
                "pos_qty":            round(pos_qty,  3),
                "uom":                it.get("stock_uom"),
                "pending_from_date":  effective_cutoff,   # informational / debug
            }

            stores_map.setdefault(wh, []).append(row)

    # Sort rows inside each warehouse: style, item_code, color, size
    for wh, wh_rows in stores_map.items():
        wh_rows.sort(
            key=lambda r: (
                (r.get("style")     or "").lower(),
                (r.get("item_code") or "").lower(),
                (r.get("color")     or "").lower(),
                _size_sort_value(r.get("size")),
            )
        )

    # Build output stores array sorted by warehouse name
    stores = [
        {"store": wh, "rows": stores_map[wh]}
        for wh in sorted(stores_map.keys(), key=lambda x: (x or "").lower())
    ]

    # Optional: also keep flat rows (some clients may still want it)
    flat_rows: list[dict] = []
    for s in stores:
        flat_rows.extend(s["rows"])

    return {
        "ok":    True,
        "mode":  mode,
        "source": "Bin - POS Invoice (consolidated_invoice IS NULL, post-reconciliation only)",
        "company":    company,
        "style":      style      or "",
        "item_code":  item_code  or "",
        "barcode":    barcode    or "",
        "qty_field_used": qty_field_used,
        "filters": {
            "custom_exclude":      custom_exclude,
            "posting_date_from":   posting_date_from_reported,
            "manual_override":     bool(manual_posting_date_from),
        },

        # BEST for UI:
        "stores": stores,

        # Backward compatible:
        "rows": flat_rows,
    }


# Backward compatible old name
@frappe.whitelist()
def stock_check(*args, **kwargs):
    return stock_check_by_item(*args, **kwargs)


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _get_color_size_map(item_codes: list[str], style_hint: str = "") -> dict[str, dict]:
    out: dict[str, dict] = {
        ic: {"style": style_hint or "", "color": "", "size": ""}
        for ic in (item_codes or [])
    }
    if not item_codes:
        return out

    iva_has_variant_of = frappe.db.has_column("Item Variant Attribute", "variant_of")
    item_has_variant_of = frappe.db.has_column("Item", "variant_of")

    placeholders = ", ".join(["%s"] * len(item_codes))

    if iva_has_variant_of:
        rows = frappe.db.sql(
            f"""
            SELECT
                parent AS item_code,
                MAX(CASE WHEN attribute='Color' THEN attribute_value END) AS color,
                MAX(CASE WHEN attribute='Size'  THEN attribute_value END) AS size,
                MAX(variant_of) AS style
            FROM `tabItem Variant Attribute`
            WHERE parent IN ({placeholders})
            GROUP BY parent
            """,
            item_codes,
            as_dict=True,
        )
        for r in rows:
            ic = r["item_code"]
            out.setdefault(ic, {"style": style_hint or "", "color": "", "size": ""})
            out[ic]["color"] = r.get("color") or ""
            out[ic]["size"]  = r.get("size")  or ""
            out[ic]["style"] = r.get("style") or out[ic].get("style") or ""
        return out

    rows = frappe.db.sql(
        f"""
        SELECT
            iva.parent AS item_code,
            MAX(CASE WHEN iva.attribute='Color' THEN iva.attribute_value END) AS color,
            MAX(CASE WHEN iva.attribute='Size'  THEN iva.attribute_value END) AS size
        FROM `tabItem Variant Attribute` iva
        WHERE iva.parent IN ({placeholders})
        GROUP BY iva.parent
        """,
        item_codes,
        as_dict=True,
    )
    for r in rows:
        ic = r["item_code"]
        out.setdefault(ic, {"style": style_hint or "", "color": "", "size": ""})
        out[ic]["color"] = r.get("color") or ""
        out[ic]["size"]  = r.get("size")  or ""

    if item_has_variant_of:
        st = frappe.get_all(
            "Item",
            filters={"name": ["in", item_codes]},
            fields=["name", "variant_of"],
        )
        for s in st:
            ic = s["name"]
            out.setdefault(ic, {"style": style_hint or "", "color": "", "size": ""})
            out[ic]["style"] = s.get("variant_of") or out[ic].get("style") or ""

    return out


def _item_code_from_barcode(barcode: str) -> str:
    barcode = (barcode or "").strip()
    if not barcode:
        frappe.throw("Empty barcode")

    code = frappe.db.get_value("Item Barcode", {"barcode": barcode}, "parent")
    if code:
        return code

    if frappe.db.has_column("Item", "barcode"):
        code = frappe.db.get_value("Item", {"barcode": barcode}, "name")
        if code:
            return code

    if frappe.db.exists("Item", barcode):
        return barcode

    try:
        item_cols = set(frappe.db.get_table_columns("POS Invoice Item") or [])
        if "barcode" in item_cols and "item_code" in item_cols:
            code = frappe.db.get_value("POS Invoice Item", {"barcode": barcode}, "item_code")
            if code:
                return code
    except Exception:
        pass

    frappe.throw(
        f"Barcode not found: {barcode}. Please add it in Item -> Barcodes (Item Barcode)."
    )