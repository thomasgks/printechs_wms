# -*- coding: utf-8 -*-
"""Shared WMS location stock movements for transfer out/in aligned with ERP Stock Entry."""

from __future__ import annotations

import frappe
from frappe import _
from frappe.utils import flt, now_datetime, cstr

BALANCE_DT = "WMS Stock Balance"
LEDGER_DT = "WMS Stock Ledger Entry"
BIN_LOCATION_DT = "WMS Bin Location"

LOCATION_KEYS = (
    "source_location",
    "from_location",
    "from_bin",
    "from_bin_location",
    "bin_location",
    "location",
)
CARTON_KEYS = (
    "source_carton",
    "from_carton",
    "carton",
    "carton_id",
)
TARGET_LOCATION_KEYS = (
    "target_location",
    "to_location",
    "to_bin",
    "to_bin_location",
    "destination_location",
    "bin_location",
    "location",
)
TARGET_CARTON_KEYS = (
    "target_carton",
    "to_carton",
    "carton",
    "carton_id",
)


def _pick_allowed_event_type(preferred: str) -> str:
    meta = frappe.get_meta(LEDGER_DT)
    if not meta.has_field("event_type"):
        return preferred
    df = meta.get_field("event_type")
    options = [o.strip() for o in (df.options or "").split("\n") if o.strip()]
    if preferred in options:
        return preferred
    for cand in ("Transfer", "Receive", "Putaway", "Adjust", "CycleCount"):
        if cand in options:
            return cand
    return options[0] if options else preferred


def resolve_bin_location(location_value: str | None, warehouse: str | None = None) -> str | None:
    s = cstr(location_value).strip()
    if not s:
        return None
    if frappe.db.exists(BIN_LOCATION_DT, s):
        return s
    name = frappe.db.get_value(BIN_LOCATION_DT, {"location_id": s}, "name")
    if name:
        return name
    if warehouse:
        name = frappe.db.get_value(
            BIN_LOCATION_DT,
            {"location_id": s, "erp_warehouse": warehouse},
            "name",
        )
        if name:
            return name
    return s


def _first_key(row: dict, keys: tuple[str, ...]) -> str:
    for key in keys:
        val = row.get(key)
        if val is not None and cstr(val).strip():
            return cstr(val).strip()
    return ""


def normalize_out_line(row: dict) -> dict:
    row = row or {}
    return {
        "item_code": cstr(row.get("item_code")).strip(),
        "qty": flt(row.get("qty")),
        "location": _first_key(row, LOCATION_KEYS),
        "carton": _first_key(row, CARTON_KEYS) or None,
    }


def validate_transfer_out_items(items, *, require_carton: bool = True) -> None:
    """Reject transfer-out payloads that would trigger warehouse auto-allocation."""
    if not items or not isinstance(items, list):
        frappe.throw(_("items is required and must be a list"))

    missing = []
    for raw in items:
        line = normalize_out_line(raw)
        if not line["item_code"] or line["qty"] <= 0:
            continue
        if not line["location"]:
            missing.append(_("item {0}: bin location is required").format(line["item_code"]))
        if require_carton and not line["carton"]:
            missing.append(_("item {0}: carton ID is required").format(line["item_code"]))

    if missing:
        frappe.throw(
            _("WMS transfer rejected — location and carton are required on every item line. "
              "Auto warehouse pick is disabled.<br>{0}").format("<br>".join(missing)),
            title=_("Missing Pick Location / Carton"),
        )


def normalize_in_line(row: dict) -> dict:
    row = row or {}
    return {
        "item_code": cstr(row.get("item_code")).strip(),
        "qty": flt(row.get("qty")),
        "location": _first_key(row, TARGET_LOCATION_KEYS),
        "carton": _first_key(row, TARGET_CARTON_KEYS) or None,
    }


def get_wms_warehouse_total(company: str, warehouse: str, item_code: str) -> float:
    rows = frappe.get_all(
        BALANCE_DT,
        filters={"company": company, "warehouse": warehouse, "item_code": item_code},
        fields=["qty"],
    )
    return sum(flt(r.get("qty")) for r in rows)


def get_erp_bin_qty(item_code: str, warehouse: str) -> float:
    return flt(frappe.db.get_value("Bin", {"item_code": item_code, "warehouse": warehouse}, "actual_qty"))


def ledger_applied_for_voucher(voucher_doctype: str, voucher_name: str) -> bool:
    return bool(
        frappe.db.exists(
            LEDGER_DT,
            {"voucher_doctype": voucher_doctype, "voucher_name": voucher_name},
        )
    )


def _balance_filters(company, warehouse, item_code, location, carton):
    filters = {
        "company": company,
        "warehouse": warehouse,
        "item_code": item_code,
        "location": location,
    }
    if carton:
        filters["carton"] = carton
    else:
        filters["carton"] = ["in", ["", None]]
    return filters


def _get_balance_row(company, warehouse, item_code, location, carton):
    if carton:
        name = frappe.db.get_value(
            BALANCE_DT,
            {"company": company, "warehouse": warehouse, "item_code": item_code, "location": location, "carton": carton},
            ["name", "qty"],
            as_dict=True,
        )
        if name:
            return name
    rows = frappe.get_all(
        BALANCE_DT,
        filters={"company": company, "warehouse": warehouse, "item_code": item_code, "location": location, "carton": ["in", ["", None]]},
        fields=["name", "qty", "carton"],
        limit=1,
    )
    return rows[0] if rows else None


def update_wms_balance(company, warehouse, item_code, location, carton, qty_delta) -> float:
    now_dt = now_datetime()
    row = _get_balance_row(company, warehouse, item_code, location, carton)
    carton_val = carton or ""

    if row:
        bal = frappe.get_doc(BALANCE_DT, row.name)
        bal.qty = flt(bal.qty) + flt(qty_delta)
        if bal.qty < 0:
            frappe.throw(
                _("Insufficient WMS stock for {0} at {1}/{2}. Available {3}, need {4}").format(
                    item_code,
                    location,
                    carton_val or "-",
                    flt(row.qty),
                    abs(flt(qty_delta)),
                )
            )
        bal.last_txn_datetime = now_dt
        bal.save(ignore_permissions=True)
        return flt(bal.qty)

    if flt(qty_delta) < 0:
        frappe.throw(_("No WMS stock row for {0} at {1}").format(item_code, location))

    bal = frappe.new_doc(BALANCE_DT)
    bal.company = company
    bal.warehouse = warehouse
    bal.item_code = item_code
    bal.location = location
    bal.carton = carton_val
    bal.qty = flt(qty_delta)
    bal.reserved_qty = 0
    bal.last_txn_datetime = now_dt
    bal.insert(ignore_permissions=True)
    return flt(bal.qty)


def insert_wms_ledger(
    *,
    company,
    item_code,
    location,
    carton,
    qty_change,
    qty_after,
    event_type,
    voucher_doctype,
    voucher_name,
    wms_txn_id,
    remarks=None,
):
    if frappe.db.exists(LEDGER_DT, {"wms_txn_id": wms_txn_id}):
        return None

    led = frappe.new_doc(LEDGER_DT)
    led.posting_datetime = now_datetime()
    led.company = company
    led.item_code = item_code
    led.location = location
    led.carton = carton or ""
    led.qty_change = flt(qty_change)
    led.qty_after = flt(qty_after)
    led.event_type = _pick_allowed_event_type(event_type)
    led.voucher_doctype = voucher_doctype
    led.voucher_name = voucher_name
    led.wms_txn_id = wms_txn_id
    if remarks:
        led.remarks = remarks
    led.insert(ignore_permissions=True)
    return led.name


def apply_line_movement(
    *,
    company,
    warehouse,
    item_code,
    location,
    carton,
    qty_delta,
    event_type,
    voucher_doctype,
    voucher_name,
    txn_id,
    remarks=None,
    update_balance=True,
):
    location = resolve_bin_location(location, warehouse)
    if not location:
        frappe.throw(_("Bin location is required for WMS movement on item {0}").format(item_code))

    existing = _get_balance_row(company, warehouse, item_code, location, carton)
    before = flt(existing.get("qty")) if existing else 0.0
    if update_balance:
        after = update_wms_balance(company, warehouse, item_code, location, carton, qty_delta)
    else:
        after = before
    insert_wms_ledger(
        company=company,
        item_code=item_code,
        location=location,
        carton=carton,
        qty_change=qty_delta,
        qty_after=after,
        event_type=event_type,
        voucher_doctype=voucher_doctype,
        voucher_name=voucher_name,
        wms_txn_id=txn_id,
        remarks=remarks,
    )
    return {"location": location, "carton": carton or "", "qty_change": qty_delta, "qty_after": after, "qty_before": before}


def _balance_rows_for_item(company, warehouse, item_code):
    return frappe.get_all(
        BALANCE_DT,
        filters={"company": company, "warehouse": warehouse, "item_code": item_code, "qty": [">", 0]},
        fields=["name", "location", "carton", "qty"],
        order_by="last_txn_datetime asc, location asc, carton asc",
    )


def _transfer_out_qty_map(voucher_doctype: str, voucher_name: str) -> dict[tuple[str, str, str], float]:
    out: dict[tuple[str, str, str], float] = {}
    for row in frappe.db.sql(
        """
        SELECT item_code, location, IFNULL(carton, '') AS carton, ABS(qty_change) AS qty
        FROM `tabWMS Stock Ledger Entry`
        WHERE voucher_doctype = %s
          AND voucher_name = %s
          AND qty_change < 0
        """,
        (voucher_doctype, voucher_name),
        as_dict=True,
    ):
        key = (cstr(row.item_code), cstr(row.location), cstr(row.carton or ""))
        out[key] = out.get(key, 0.0) + flt(row.qty)
    return out


def attach_desktop_mr_ledger_to_voucher(
    material_request: str,
    voucher_doctype: str,
    voucher_name: str,
) -> int:
    """Link desktop offline-sync pick rows to the voucher without changing balances."""
    material_request = cstr(material_request).strip()
    if not material_request:
        return 0

    rows = frappe.db.sql(
        """
        SELECT name
        FROM `tabWMS Stock Ledger Entry`
        WHERE remarks = %s
          AND qty_change < 0
          AND (voucher_name IS NULL OR voucher_name = '' OR voucher_name = %s)
          AND IFNULL(wms_txn_id, '') LIKE 'DESKTOP-STX-%%'
        """,
        (material_request, material_request),
        as_dict=True,
    )
    for row in rows:
        frappe.db.set_value(
            LEDGER_DT,
            row.name,
            {"voucher_doctype": voucher_doctype, "voucher_name": voucher_name},
            update_modified=False,
        )
    return len(rows)


def _is_insufficient_wms_error(exc: Exception) -> bool:
    msg = cstr(exc)
    return "Insufficient WMS stock" in msg or "No WMS stock row" in msg


def _apply_out_line_with_historical(
    *,
    company,
    from_warehouse,
    line: dict,
    voucher_doctype,
    voucher_name,
    txn_id,
    remarks=None,
    allow_historical=False,
    qty=None,
):
    qty = flt(qty if qty is not None else line["qty"])
    if qty <= 1e-9:
        return None, None

    kwargs = dict(
        company=company,
        warehouse=from_warehouse,
        item_code=line["item_code"],
        location=line["location"],
        carton=line["carton"],
        qty_delta=-qty,
        event_type="Transfer",
        voucher_doctype=voucher_doctype,
        voucher_name=voucher_name,
        txn_id=txn_id,
        remarks=remarks,
    )
    try:
        return apply_line_movement(**kwargs, update_balance=True), "balance"
    except Exception as exc:
        if not allow_historical or not _is_insufficient_wms_error(exc):
            raise
        return apply_line_movement(**kwargs, update_balance=False), "ledger_only"


def allocate_and_apply_out(
    *,
    company,
    warehouse,
    item_code,
    qty,
    voucher_doctype,
    voucher_name,
    txn_prefix,
    remarks=None,
):
    remaining = flt(qty)
    if remaining <= 0:
        return []

    applied = []
    rows = _balance_rows_for_item(company, warehouse, item_code)
    available = sum(flt(r.qty) for r in rows)
    if available + 1e-9 < remaining:
        frappe.throw(
            _("Insufficient WMS stock for {0} in {1}. Available {2}, required {3}").format(
                item_code, warehouse, available, remaining
            )
        )

    for row in rows:
        if remaining <= 1e-9:
            break
        take = min(flt(row.qty), remaining)
        if take <= 1e-9:
            continue
        txn_id = f"{txn_prefix}:{item_code}:{row.location}:{row.carton or ''}:{len(applied)}"
        applied.append(
            apply_line_movement(
                company=company,
                warehouse=warehouse,
                item_code=item_code,
                location=row.location,
                carton=row.carton or None,
                qty_delta=-take,
                event_type="Transfer",
                voucher_doctype=voucher_doctype,
                voucher_name=voucher_name,
                txn_id=txn_id,
                remarks=remarks,
            )
        )
        remaining -= take

    return applied


def default_receive_location(warehouse: str) -> str | None:
    loc = frappe.db.get_value(
        BIN_LOCATION_DT,
        {"erp_warehouse": warehouse, "status": "Active"},
        "name",
        order_by="location_id asc",
    )
    return loc


def apply_transfer_out(
    *,
    company,
    from_warehouse,
    items,
    voucher_doctype,
    voucher_name,
    external_ref=None,
    remarks=None,
    strict_location=True,
    require_carton=True,
    material_request=None,
    allow_historical=False,
):
    material_request = cstr(material_request).strip()
    use_delta_mode = bool(material_request or allow_historical)

    if not use_delta_mode and ledger_applied_for_voucher(voucher_doctype, voucher_name):
        return {"ok": True, "skipped": True, "reason": "ledger_exists", "lines": []}

    if strict_location:
        validate_transfer_out_items(items, require_carton=require_carton)

    attached_desktop = 0
    if material_request:
        attached_desktop = attach_desktop_mr_ledger_to_voucher(
            material_request, voucher_doctype, voucher_name
        )

    existing_out = _transfer_out_qty_map(voucher_doctype, voucher_name) if use_delta_mode else {}

    txn_prefix = cstr(external_ref or voucher_name)
    applied = []
    ledger_only = []
    skipped_existing = []
    pooled: dict[str, float] = {}
    line_index = 0

    for raw in items or []:
        line = normalize_out_line(raw)
        if not line["item_code"] or line["qty"] <= 0:
            continue
        if line["location"]:
            if require_carton and not line["carton"]:
                frappe.throw(
                    _("Carton ID is required for item {0}. Auto warehouse pick is disabled.").format(
                        line["item_code"]
                    )
                )
            loc = resolve_bin_location(line["location"], from_warehouse)
            key = (line["item_code"], loc, cstr(line["carton"] or ""))
            target_qty = flt(line["qty"])
            remaining = target_qty
            if use_delta_mode:
                remaining = target_qty - flt(existing_out.get(key))
                if remaining <= 1e-9:
                    skipped_existing.append(
                        {"item_code": line["item_code"], "location": loc, "carton": line["carton"], "qty": target_qty}
                    )
                    continue

            txn_id = f"{txn_prefix}:{line['item_code']}:{loc}:{line['carton'] or ''}:OUT:{line_index}"
            line_index += 1
            result, mode = _apply_out_line_with_historical(
                company=company,
                from_warehouse=from_warehouse,
                line=line,
                voucher_doctype=voucher_doctype,
                voucher_name=voucher_name,
                txn_id=txn_id,
                remarks=remarks,
                allow_historical=allow_historical,
                qty=remaining,
            )
            if result:
                applied.append(result)
                if mode == "ledger_only":
                    ledger_only.append(result)
                if use_delta_mode:
                    existing_out[key] = flt(existing_out.get(key)) + remaining
        elif strict_location:
            frappe.throw(_("source_location is required for item {0}").format(line["item_code"]))
        else:
            pooled[line["item_code"]] = pooled.get(line["item_code"], 0) + line["qty"]

    if pooled and strict_location:
        frappe.throw(
            _("WMS transfer rejected — every item line must include bin location and carton. "
              "Auto warehouse pick is disabled."),
            title=_("Missing Pick Location / Carton"),
        )

    for item_code, qty in pooled.items():
        applied.extend(
            allocate_and_apply_out(
                company=company,
                warehouse=from_warehouse,
                item_code=item_code,
                qty=qty,
                voucher_doctype=voucher_doctype,
                voucher_name=voucher_name,
                txn_prefix=txn_prefix,
                remarks=remarks,
            )
        )

    if use_delta_mode and not applied and (skipped_existing or attached_desktop):
        return {
            "ok": True,
            "skipped": True,
            "reason": "desktop_picks_linked",
            "attached_desktop": attached_desktop,
            "skipped_existing": skipped_existing,
            "lines": [],
        }

    return {
        "ok": True,
        "skipped": False,
        "attached_desktop": attached_desktop,
        "ledger_only_count": len(ledger_only),
        "skipped_existing_count": len(skipped_existing),
        "lines": applied,
    }


def apply_transfer_in(
    *,
    company,
    to_warehouse,
    items,
    voucher_doctype,
    voucher_name,
    external_ref=None,
    remarks=None,
    strict_location=False,
):
    if ledger_applied_for_voucher(voucher_doctype, voucher_name):
        return {"ok": True, "skipped": True, "reason": "ledger_exists", "lines": []}

    txn_prefix = cstr(external_ref or voucher_name)
    applied = []

    for raw in items or []:
        line = normalize_in_line(raw)
        if not line["item_code"] or line["qty"] <= 0:
            continue

        location = line["location"]
        if not location:
            if strict_location:
                frappe.throw(_("target_location is required for item {0}").format(line["item_code"]))
            location = default_receive_location(to_warehouse)
            if not location:
                frappe.throw(
                    _("No receive bin location found for warehouse {0}. Send target_location in payload.").format(
                        to_warehouse
                    )
                )

        loc = resolve_bin_location(location, to_warehouse)
        txn_id = f"{txn_prefix}:{line['item_code']}:{loc}:{line['carton'] or ''}:IN"
        applied.append(
            apply_line_movement(
                company=company,
                warehouse=to_warehouse,
                item_code=line["item_code"],
                location=location,
                carton=line["carton"],
                qty_delta=line["qty"],
                event_type="Transfer",
                voucher_doctype=voucher_doctype,
                voucher_name=voucher_name,
                txn_id=txn_id,
                remarks=remarks,
            )
        )

    return {"ok": True, "skipped": False, "lines": applied}


def reconcile_stock_entry_transfer(stock_entry_name: str, *, dry_run: bool = False) -> dict:
    """Apply missing WMS transfer-out rows for an existing submitted Stock Entry."""
    se = frappe.get_doc("Stock Entry", stock_entry_name)
    if int(se.docstatus or 0) != 1:
        return {"ok": False, "error": "not_submitted", "stock_entry": stock_entry_name}

    if ledger_applied_for_voucher("Stock Entry", stock_entry_name):
        return {"ok": True, "skipped": True, "reason": "ledger_exists", "stock_entry": stock_entry_name}

    items = []
    from_wh = None
    to_wh = None
    for row in se.items or []:
        from_wh = from_wh or row.s_warehouse
        to_wh = to_wh or row.t_warehouse
        items.append({"item_code": row.item_code, "qty": row.qty})

    if not from_wh or not items:
        return {"ok": False, "error": "no_items", "stock_entry": stock_entry_name}

    result = {
        "ok": True,
        "stock_entry": stock_entry_name,
        "from_warehouse": from_wh,
        "to_warehouse": to_wh,
        "item_count": len(items),
        "dry_run": dry_run,
    }

    if dry_run:
        gaps = []
        for row in items:
            wms_total = get_wms_warehouse_total(se.company, from_wh, row["item_code"])
            erp_qty = get_erp_bin_qty(row["item_code"], from_wh)
            gaps.append(
                {
                    "item_code": row["item_code"],
                    "transfer_qty": flt(row["qty"]),
                    "wms_total": wms_total,
                    "erp_bin": erp_qty,
                    "wms_excess": max(0, wms_total - erp_qty),
                }
            )
        result["gaps"] = gaps
        return result

    return {
        "ok": False,
        "error": "missing_location_carton",
        "stock_entry": stock_entry_name,
        "message": _(
            "Cannot apply WMS transfer-out without bin location and carton on each item line. "
            "Auto warehouse pick is disabled."
        ),
    }


@frappe.whitelist(methods=["POST"])
def reconcile_post_count_transfers(stock_entries=None, posting_date_from=None, dry_run=0):
    """Reconcile WMS transfer-out for desktop pushes after cycle count."""
    dry_run = int(dry_run or 0)

    names = []
    if stock_entries:
        if isinstance(stock_entries, str):
            stock_entries = frappe.parse_json(stock_entries)
        names = list(stock_entries or [])

    if not names:
        filters = {"docstatus": 1, "remarks": ["like", "%Push from WMS Desktop%"]}
        if posting_date_from:
            filters["posting_date"] = [">=", posting_date_from]
        names = frappe.get_all("Stock Entry", filters=filters, pluck="name", order_by="posting_date asc, name asc")

    results = []
    for name in names:
        results.append(reconcile_stock_entry_transfer(name, dry_run=bool(dry_run)))

    return {"ok": True, "count": len(results), "results": results}
