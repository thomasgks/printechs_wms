# -*- coding: utf-8 -*-
"""Apply WMS auto-pick corrections from confirmed desktop picks file."""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path

import openpyxl
import frappe
from frappe import _
from frappe.utils import cstr, flt, now_datetime

from printechs_wms.api.wms_stock_movement import apply_line_movement

TRANSACTIONS_PATH = Path(__file__).resolve().parents[2] / "doc" / "Transactions.xlsx"
CONFIRMED_PICKS_PATH = Path(__file__).resolve().parents[2] / "doc" / "WMS_Desktop_Confirmed_Picks.xlsx"
EXCLUDED_MRS = frozenset({"MR-2026-03975"})
CORRECTION_REMARK = "Auto-pick correction {mr} / {ste}"
HISTORICAL_REMARK = "Auto-pick correction (historical) {mr} / {ste}"
PARTIAL_REMARK = "Auto-pick correction (partial) {mr} / {ste}"
CLEANUP_REMARK = "Cleanup auto-pick correction {mr} / {ste}"


def _short_txn_id(ste: str, prefix: str, seq: int) -> str:
    return f"{ste}:{prefix}:{seq}"[:140]


def _cleanup_txn_id(ste: str, key: tuple[str, str, str], seq: int) -> str:
    tag = frappe.generate_hash(length=8)
    raw = f"{ste}:CLN:{key[0]}:{key[1]}:{key[2]}:{seq}:{tag}"
    return raw[:140]


def _transfer_out_map(stock_entry_name: str) -> dict[tuple[str, str, str], float]:
    out: dict[tuple[str, str, str], float] = {}
    for r in frappe.db.sql(
        """
        SELECT item_code, location, IFNULL(carton, '') AS carton, ABS(qty_change) AS qty
        FROM `tabWMS Stock Ledger Entry`
        WHERE voucher_doctype = 'Stock Entry'
          AND voucher_name = %s
          AND event_type = 'Transfer'
          AND qty_change < 0
        """,
        stock_entry_name,
        as_dict=True,
    ):
        key = (cstr(r.item_code), cstr(r.location), cstr(r.carton or ""))
        out[key] = out.get(key, 0.0) + flt(r.qty)
    return out


def _desktop_ste_map() -> dict[str, str]:
    rows = frappe.get_all(
        "Stock Entry",
        filters={"docstatus": 1, "remarks": ["like", "%Push from WMS Desktop%"]},
        fields=["name", "custom_material_request"],
    )
    out: dict[str, str] = {}
    for row in rows:
        mr = cstr(row.custom_material_request).strip()
        if mr:
            out[mr] = row.name
    return out


def _default_picks_path() -> Path:
    if TRANSACTIONS_PATH.exists():
        return TRANSACTIONS_PATH
    return CONFIRMED_PICKS_PATH


def load_confirmed_picks(path: Path | None = None, *, only_with_ste: bool = False) -> dict[str, list[dict]]:
    path = path or _default_picks_path()
    if not path.exists():
        frappe.throw(_("Confirmed picks file not found: {0}").format(path))

    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    ws = wb["ConfirmedPicks"] if "ConfirmedPicks" in wb.sheetnames else wb.active
    rows = list(ws.iter_rows(values_only=True))
    header = rows[0]
    idx = {h: i for i, h in enumerate(header)}
    ste_map = _desktop_ste_map()

    by_mr: dict[str, list[dict]] = defaultdict(list)

    if "reference_doc" in idx:
        agg: dict[tuple[str, str, str, str], float] = defaultdict(float)
        for row in rows[1:]:
            if not row:
                continue
            if cstr(row[idx.get("transaction_type", "")]).strip() not in ("", "Picking"):
                if "transaction_type" in idx and cstr(row[idx["transaction_type"]]).strip() != "Picking":
                    continue
            if "reference_doc_type" in idx and cstr(row[idx["reference_doc_type"]]).strip() != "Material Request":
                continue
            mr = cstr(row[idx["reference_doc"]]).strip()
            item = cstr(row[idx["item_code"]]).strip()
            loc = cstr(row[idx["bin_location"]]).strip()
            carton = cstr(row[idx["carton_id"]]).strip()
            if not mr or not item or not loc or not carton:
                continue
            if only_with_ste and mr not in ste_map:
                continue
            agg[(mr, item, loc, carton)] += abs(flt(row[idx["qty_change"]]))
        for (mr, item, loc, carton), qty in agg.items():
            if qty <= 0:
                continue
            by_mr[mr].append(
                {
                    "item_code": item,
                    "location": loc,
                    "carton": carton,
                    "qty": qty,
                    "stock_entry": ste_map.get(mr, ""),
                }
            )
    else:
        for row in rows[1:]:
            if not row:
                continue
            mr = cstr(row[idx["material_request"]]).strip()
            if not mr:
                continue
            if only_with_ste and mr not in ste_map:
                continue
            by_mr[mr].append(
                {
                    "item_code": cstr(row[idx["item_code"]]).strip(),
                    "location": cstr(row[idx["bin_location"]]).strip(),
                    "carton": cstr(row[idx["carton_id"]]).strip(),
                    "qty": flt(row[idx["desktop_qty"]]),
                    "stock_entry": cstr(row[idx.get("stock_entry", "")]).strip() or ste_map.get(mr, ""),
                }
            )
    wb.close()
    return by_mr


def aggregate_target(picks: list[dict]) -> dict[tuple[str, str, str], float]:
    target: dict[tuple[str, str, str], float] = {}
    for p in picks:
        if not p["item_code"] or not p["location"] or not p["carton"] or flt(p["qty"]) <= 0:
            frappe.throw(_("Invalid pick line for item {0}").format(p.get("item_code")))
        key = (p["item_code"], p["location"], p["carton"])
        target[key] = target.get(key, 0.0) + flt(p["qty"])
    return target


def _ste_item_qty_map(stock_entry_name: str) -> dict[str, float]:
    se = frappe.get_doc("Stock Entry", stock_entry_name)
    return {cstr(r.item_code): flt(r.qty) for r in se.items if r.s_warehouse}


def ste_aligned_picks(picks: list[dict], stock_entry_name: str) -> list[dict]:
    """Desktop picks limited to STE items; per-item qty capped to STE line qty."""
    ste_items = _ste_item_qty_map(stock_entry_name)
    erp_out = _transfer_out_map(stock_entry_name)
    by_item: dict[str, list[dict]] = defaultdict(list)
    for p in picks:
        item = cstr(p["item_code"])
        if item not in ste_items:
            continue
        by_item[item].append(
            {
                "item_code": item,
                "location": cstr(p["location"]),
                "carton": cstr(p["carton"]),
                "qty": flt(p["qty"]),
                "stock_entry": stock_entry_name,
            }
        )

    aligned: list[dict] = []
    for item, ste_q in ste_items.items():
        lines = by_item.get(item) or []
        if not lines:
            continue
        total_pick = sum(flt(x["qty"]) for x in lines)
        if abs(total_pick - ste_q) <= 1e-6:
            aligned.extend(lines)
            continue
        if total_pick < ste_q:
            aligned.extend(lines)
            continue

        def sort_key(line: dict) -> tuple:
            key = (line["item_code"], line["location"], line["carton"])
            has_erp = 1 if flt(erp_out.get(key)) > 0 else 0
            return (-has_erp, line["location"], line["carton"])

        remaining = ste_q
        for line in sorted(lines, key=sort_key):
            if remaining <= 1e-9:
                break
            take = min(flt(line["qty"]), remaining)
            if take <= 1e-9:
                continue
            aligned.append({**line, "qty": take})
            remaining -= take
    return aligned


def ste_aligned_target(picks: list[dict], stock_entry_name: str) -> dict[tuple[str, str, str], float]:
    return aggregate_target(ste_aligned_picks(picks, stock_entry_name))


def verify_correction(stock_entry_name: str, target: dict[tuple[str, str, str], float]) -> dict:
    mismatches = []
    for key, tqty in target.items():
        net = flt(
            frappe.db.sql(
                """
                SELECT SUM(qty_change)
                FROM `tabWMS Stock Ledger Entry`
                WHERE voucher_doctype = 'Stock Entry'
                  AND voucher_name = %s
                  AND item_code = %s
                  AND location = %s
                  AND IFNULL(carton, '') = %s
                """,
                (stock_entry_name, key[0], key[1], key[2]),
            )[0][0]
        )
        if abs(net + flt(tqty)) > 1e-6:
            mismatches.append(
                {
                    "item_code": key[0],
                    "location": key[1],
                    "carton": key[2],
                    "target_out": flt(tqty),
                    "net_ledger": net,
                }
            )
    return {"ok": not mismatches, "mismatch_count": len(mismatches), "mismatches": mismatches[:20]}



def _correction_adjust_net_map(stock_entry_name: str, material_request: str) -> dict[tuple[str, str, str], float]:
    nets: dict[tuple[str, str, str], float] = {}
    patterns = (
        f"%Auto-pick correction {material_request}%",
        f"%Auto-pick correction (partial) {material_request}%",
        f"%Auto-pick correction (historical) {material_request}%",
        f"%Rollback auto-pick correction {material_request}%",
        f"%Cleanup auto-pick correction {material_request}%",
    )
    for pattern in patterns:
        for r in frappe.db.sql(
            """
            SELECT item_code, location, IFNULL(carton, '') AS carton, SUM(qty_change) AS net
            FROM `tabWMS Stock Ledger Entry`
            WHERE voucher_name = %s AND event_type = 'Adjust' AND IFNULL(remarks, '') LIKE %s
            GROUP BY item_code, location, carton
            """,
            (stock_entry_name, pattern),
            as_dict=True,
        ):
            key = (cstr(r.item_code), cstr(r.location), cstr(r.carton or ""))
            nets[key] = nets.get(key, 0.0) + flt(r.net)
    return nets


def cleanup_correction_artifacts(material_request: str, picks: list[dict] | None = None) -> dict:
    if picks is None:
        picks = load_confirmed_picks().get(material_request) or []
    ste = next((p.get("stock_entry") for p in picks if p.get("stock_entry")), None)
    if not ste:
        ste = frappe.db.get_value(
            "Stock Entry",
            {"custom_material_request": material_request, "docstatus": 1, "remarks": ["like", "%Push from WMS Desktop%"]},
            "name",
        )
    if not ste:
        return {"ok": False, "material_request": material_request, "error": "no_ste"}

    nets = _correction_adjust_net_map(ste, material_request)
    to_fix = [(k, v) for k, v in nets.items() if abs(v) > 1e-9]
    if not to_fix:
        return {"ok": True, "material_request": material_request, "stock_entry": ste, "cleaned": 0}

    se = frappe.get_doc("Stock Entry", ste)
    from_wh = next((r.s_warehouse for r in se.items if r.s_warehouse), None)
    remark = CLEANUP_REMARK.format(mr=material_request, ste=ste)
    # Apply restores before deducts so balance checks pass.
    to_fix.sort(key=lambda kv: (-1 if -flt(kv[1]) > 0 else 1, kv[0]))
    applied, errors = [], []
    for i, (key, net) in enumerate(to_fix):
        try:
            apply_line_movement(
            company=se.company,
            warehouse=from_wh,
            item_code=key[0],
            location=key[1],
            carton=key[2] or None,
            qty_delta=-flt(net),
            event_type="Adjust",
            voucher_doctype="Stock Entry",
            voucher_name=ste,
            txn_id=_cleanup_txn_id(ste, key, i),
            remarks=remark,
            )
            applied.append({"key": key, "net": net})
        except Exception as exc:
            try:
                apply_line_movement(
                    company=se.company,
                    warehouse=from_wh,
                    item_code=key[0],
                    location=key[1],
                    carton=key[2] or None,
                    qty_delta=-flt(net),
                    event_type="Adjust",
                    voucher_doctype="Stock Entry",
                    voucher_name=ste,
                    txn_id=_cleanup_txn_id(ste, key, i),
                    remarks=remark + " (ledger-only)",
                    update_balance=False,
                )
                applied.append({"key": key, "net": net, "ledger_only": True})
            except Exception as exc2:
                errors.append({"key": key, "net": net, "error": cstr(exc), "ledger_only_error": cstr(exc2)})
    remaining = _correction_adjust_net_map(ste, material_request)
    remaining_nets = {k: v for k, v in remaining.items() if abs(v) > 1e-9}
    return {
        "ok": not remaining_nets and not errors,
        "material_request": material_request,
        "stock_entry": ste,
        "cleaned": len(applied),
        "errors": errors,
        "remaining_nets": {f"{k[0]}|{k[1]}|{k[2]}": v for k, v in list(remaining_nets.items())[:10]},
    }


def preflight_correction(material_request: str, picks: list[dict] | None = None) -> dict:
    if picks is None:
        picks = load_confirmed_picks().get(material_request) or []
    if not picks:
        return {"ok": False, "material_request": material_request, "error": "no_picks"}

    ste = next((p.get("stock_entry") for p in picks if p.get("stock_entry")), None)
    if not ste:
        ste = frappe.db.get_value(
            "Stock Entry",
            {"custom_material_request": material_request, "docstatus": 1, "remarks": ["like", "%Push from WMS Desktop%"]},
            "name",
        )
    target = aggregate_target(picks)
    erp_out = _transfer_out_map(ste)
    if abs(sum(target.values()) - sum(erp_out.values())) > 1e-6:
        return {
            "ok": False,
            "material_request": material_request,
            "error": "qty_total_mismatch",
            "desktop": sum(target.values()),
            "erp": sum(erp_out.values()),
        }

    se = frappe.get_doc("Stock Entry", ste)
    from_wh = next((r.s_warehouse for r in se.items if r.s_warehouse), None)
    balances: dict[tuple[str, str, str], float] = {}
    for key in set(target) | set(erp_out):
        balances[key] = flt(
            frappe.db.get_value(
                "WMS Stock Balance",
                {"item_code": key[0], "location": key[1], "carton": key[2], "warehouse": from_wh},
                "qty",
            )
        )

    deltas = []
    for key in set(target) | set(erp_out):
        delta = flt(target.get(key)) - flt(erp_out.get(key))
        if abs(delta) <= 1e-9:
            continue
        deltas.append((key, -delta))

    blockers = []
    for key, qty_delta in sorted(deltas, key=lambda x: (-1 if x[1] > 0 else 1, x[0])):
        balances[key] = balances.get(key, 0.0) + qty_delta
        if balances[key] < -1e-9:
            blockers.append(
                {"item_code": key[0], "location": key[1], "carton": key[2], "qty_delta": qty_delta, "balance_after": balances[key]}
            )
    return {
        "ok": not blockers,
        "material_request": material_request,
        "stock_entry": ste,
        "correction_lines": len(deltas),
        "blockers": blockers[:20],
    }





def _ledger_net_map(stock_entry_name: str) -> dict[tuple[str, str, str], float]:
    nets: dict[tuple[str, str, str], float] = {}
    for r in frappe.db.sql(
        """
        SELECT item_code, location, IFNULL(carton, '') AS carton, SUM(qty_change) AS net
        FROM `tabWMS Stock Ledger Entry`
        WHERE voucher_doctype = 'Stock Entry' AND voucher_name = %s
        GROUP BY item_code, location, carton
        """,
        stock_entry_name,
        as_dict=True,
    ):
        key = (cstr(r.item_code), cstr(r.location), cstr(r.carton or ""))
        nets[key] = flt(r.net)
    return nets


def _remaining_correction_deltas(
    target: dict[tuple[str, str, str], float],
    erp_out: dict[tuple[str, str, str], float],
    ledger_net: dict[tuple[str, str, str], float],
) -> list[dict]:
    deltas = []
    for key in set(target) | set(erp_out):
        desired_net = -flt(target.get(key))
        current_net = flt(ledger_net.get(key))
        remaining = desired_net - current_net
        if abs(remaining) <= 1e-9:
            continue
        deltas.append(
            {
                "item_code": key[0],
                "location": key[1],
                "carton": key[2],
                "erp_out": flt(erp_out.get(key)),
                "target": flt(target.get(key)),
                "qty_delta": remaining,
            }
        )
    return deltas


def _correction_deltas_for_mr(
    material_request: str,
    picks: list[dict],
    *,
    use_remaining: bool = False,
) -> dict:
    ste = next((p.get("stock_entry") for p in picks if p.get("stock_entry")), None)
    if not ste:
        ste = frappe.db.get_value(
            "Stock Entry",
            {
                "custom_material_request": material_request,
                "docstatus": 1,
                "remarks": ["like", "%Push from WMS Desktop%"],
            },
            "name",
        )
    if not ste:
        frappe.throw(_("No submitted desktop Stock Entry for {0}").format(material_request))

    target = aggregate_target(picks)
    erp_out = _transfer_out_map(ste)
    total_target = sum(target.values())
    total_erp = sum(erp_out.values())
    if abs(total_target - total_erp) > 1e-6:
        frappe.throw(
            _("Total qty mismatch for {0}: desktop {1}, ERP transfer-out {2}").format(
                material_request, total_target, total_erp
            )
        )

    if use_remaining:
        deltas = _remaining_correction_deltas(target, erp_out, _ledger_net_map(ste))
    else:
        deltas = []
        for key in set(target) | set(erp_out):
            delta = flt(target.get(key)) - flt(erp_out.get(key))
            if abs(delta) <= 1e-9:
                continue
            deltas.append(
                {
                    "item_code": key[0],
                    "location": key[1],
                    "carton": key[2],
                    "erp_out": flt(erp_out.get(key)),
                    "target": flt(target.get(key)),
                    "qty_delta": -delta,
                }
            )

    se = frappe.get_doc("Stock Entry", ste)
    from_wh = next((r.s_warehouse for r in se.items if r.s_warehouse), None)
    return {
        "material_request": material_request,
        "stock_entry": ste,
        "target": target,
        "deltas": deltas,
        "company": se.company,
        "warehouse": from_wh,
    }


def _balance_now(warehouse: str, item_code: str, location: str, carton: str) -> float:
    return flt(
        frappe.db.get_value(
            "WMS Stock Balance",
            {"item_code": item_code, "location": location, "carton": carton, "warehouse": warehouse},
            "qty",
        )
    )


def _simulate_safe_deltas(warehouse: str, deltas: list[dict]) -> tuple[list[dict], list[dict]]:
    balances: dict[tuple[str, str, str], float] = {}
    keys = {(d["item_code"], d["location"], d["carton"]) for d in deltas}
    for key in keys:
        balances[key] = _balance_now(warehouse, key[0], key[1], key[2])

    safe, skipped = [], []
    for line in sorted(deltas, key=lambda d: (-1 if d["qty_delta"] > 0 else 1, d["item_code"], d["location"], d["carton"])):
        key = (line["item_code"], line["location"], line["carton"])
        after = balances.get(key, 0.0) + flt(line["qty_delta"])
        if after < -1e-9:
            skipped.append({**line, "balance_after": after})
            continue
        balances[key] = after
        safe.append(line)
    return safe, skipped


def _apply_correction_lines(
    *,
    material_request: str,
    stock_entry: str,
    company: str,
    warehouse: str,
    lines: list[dict],
    remark: str,
    txn_prefix: str,
    historical: bool = False,
) -> dict:
    applied, ledger_only, errors = [], [], []
    for i, line in enumerate(lines):
        txn_id = _short_txn_id(stock_entry, txn_prefix, i)
        kwargs = dict(
            company=company,
            warehouse=warehouse,
            item_code=line["item_code"],
            location=line["location"],
            carton=line["carton"] or None,
            qty_delta=line["qty_delta"],
            event_type="Adjust",
            voucher_doctype="Stock Entry",
            voucher_name=stock_entry,
            txn_id=txn_id,
            remarks=remark,
        )
        try:
            apply_line_movement(**kwargs)
            applied.append(line)
        except Exception as exc:
            if not historical or flt(line["qty_delta"]) >= 0:
                errors.append({"line": line, "error": cstr(exc)})
                continue
            try:
                apply_line_movement(**kwargs, update_balance=False)
                ledger_only.append(line)
            except Exception as exc2:
                errors.append({"line": line, "error": cstr(exc), "ledger_only_error": cstr(exc2)})
    return {"applied": applied, "ledger_only": ledger_only, "errors": errors}


def apply_auto_pick_correction_for_mr(
    material_request: str,
    picks: list[dict] | None = None,
    *,
    dry_run: bool = False,
    partial: bool = False,
    historical: bool = False,
) -> dict:
    if picks is None:
        by_mr = load_confirmed_picks()
        picks = by_mr.get(material_request) or []
    if not picks:
        frappe.throw(_("No confirmed picks for {0}").format(material_request))

    ste = next((p.get("stock_entry") for p in picks if p.get("stock_entry")), None)
    if not ste:
        ste = frappe.db.get_value(
            "Stock Entry",
            {
                "custom_material_request": material_request,
                "docstatus": 1,
                "remarks": ["like", "%Push from WMS Desktop%"],
            },
            "name",
        )
    if not ste:
        frappe.throw(_("No submitted desktop Stock Entry for {0}").format(material_request))

    try:
        ctx = _correction_deltas_for_mr(material_request, picks, use_remaining=False)
    except Exception as exc:
        return {"ok": False, "material_request": material_request, "error": cstr(exc)}

    ste = ctx["stock_entry"]
    target = ctx["target"]
    deltas = ctx["deltas"]

    existing = verify_correction(ste, target)
    if existing.get("ok"):
        return {
            "ok": True,
            "skipped": True,
            "reason": "already_correct",
            "material_request": material_request,
            "stock_entry": ste,
            "verify": existing,
        }

    if partial:
        safe, skipped = _simulate_safe_deltas(ctx["warehouse"], deltas)
        remark = PARTIAL_REMARK.format(mr=material_request, ste=ste)
        txn_prefix = "PAR"
        lines_to_apply = safe
    elif historical:
        remark = HISTORICAL_REMARK.format(mr=material_request, ste=ste)
        txn_prefix = "HIS"
        lines_to_apply = sorted(
            deltas,
            key=lambda d: (-1 if d["qty_delta"] > 0 else 1, d["item_code"], d["location"], d["carton"]),
        )
    else:
        remark = CORRECTION_REMARK.format(mr=material_request, ste=ste)
        txn_prefix = "COR"
        lines_to_apply = sorted(
            deltas,
            key=lambda d: (-1 if d["qty_delta"] > 0 else 1, d["item_code"], d["location"], d["carton"]),
        )

    result = {
        "ok": True,
        "material_request": material_request,
        "stock_entry": ste,
        "correction_lines": len(deltas),
        "lines_to_apply": len(lines_to_apply),
        "partial": partial,
        "historical": historical,
        "dry_run": dry_run,
    }
    if partial:
        result["skipped_lines"] = len(deltas) - len(lines_to_apply)
    if dry_run:
        result["lines"] = lines_to_apply
        return result

    if not historical and not partial:
        pf = preflight_correction(material_request, picks)
        if not pf.get("ok"):
            return {
                "ok": False,
                "material_request": material_request,
                "stock_entry": ste,
                "error": pf.get("error") or "preflight_failed",
                "preflight": pf,
            }

    if not lines_to_apply and not historical:
        return {
            "ok": False,
            "material_request": material_request,
            "stock_entry": ste,
            "error": "nothing_to_apply",
        }

    apply_res = _apply_correction_lines(
        material_request=material_request,
        stock_entry=ste,
        company=ctx["company"],
        warehouse=ctx["warehouse"],
        lines=lines_to_apply,
        remark=remark,
        txn_prefix=txn_prefix,
        historical=historical,
    )
    result.update(
        {
            "applied": len(apply_res["applied"]),
            "ledger_only": len(apply_res["ledger_only"]),
            "apply_errors": apply_res["errors"][:5],
        }
    )

    if apply_res["errors"] and not historical:
        cleanup_correction_artifacts(material_request, picks)
        frappe.db.commit()
        result["ok"] = False
        result["error"] = "apply_failed"
        return result

    verify = verify_correction(ste, target)
    result["verify"] = verify
    if verify.get("ok"):
        return result

    if partial:
        result["ok"] = bool(apply_res["applied"] or apply_res["ledger_only"])
        result["reason"] = "partial_applied"
        return result

    if historical and apply_res["errors"]:
        result["ok"] = False
        result["error"] = "historical_apply_failed"
        return result

    if historical:
        result["ok"] = False
        result["error"] = "verification_failed"
        result["verify_mismatches"] = verify.get("mismatches", [])[:5]
        return result

    cleanup_correction_artifacts(material_request, picks)
    result["ok"] = False
    result["error"] = "verification_failed"
    return result


def _mr_sort_key(mr: str) -> tuple:
    # Shared-carton post-cycle MRs first, problematic MR last in that group.
    priority = {
        "MR-2026-04836": 1,
        "MR-2026-04837": 2,
        "MR-2026-04700": 3,
        "MR-2026-04725": 4,
        "MR-2026-04833": 99,
        "MR-2026-03975": 98,
    }
    return (priority.get(mr, 50), mr)


@frappe.whitelist(methods=["POST"])
def apply_confirmed_picks_batch(material_requests=None, dry_run: int = 0):
    by_mr = load_confirmed_picks()
    if material_requests:
        if isinstance(material_requests, str):
            material_requests = frappe.parse_json(material_requests)
        mrs = [cstr(m).strip() for m in material_requests if cstr(m).strip()]
    else:
        mrs = sorted(by_mr.keys(), key=_mr_sort_key)

    results = []
    for mr in mrs:
        try:
            res = apply_auto_pick_correction_for_mr(mr, by_mr.get(mr), dry_run=bool(int(dry_run or 0)))
        except Exception as exc:
            res = {"ok": False, "material_request": mr, "error": cstr(exc)}
        results.append(res)
        if not bool(int(dry_run or 0)):
            frappe.db.commit()

    ok = sum(1 for r in results if r.get("ok"))
    failed = [r for r in results if not r.get("ok")]
    skipped = sum(1 for r in results if r.get("skipped"))
    return {
        "ok": True,
        "total": len(results),
        "success": ok,
        "skipped": skipped,
        "failed": len(failed),
        "results": results,
        "failures": failed,
    }


def _correction_remark_patterns(material_request: str) -> tuple[str, ...]:
    return (
        f"%Auto-pick correction {material_request}%",
        f"%Auto-pick correction (partial) {material_request}%",
        f"%Auto-pick correction (historical) {material_request}%",
        f"%Rollback auto-pick correction {material_request}%",
        f"%Cleanup auto-pick correction {material_request}%",
    )


def _rebuild_balance_for_keys(company: str, warehouse: str, keys: set[tuple[str, str, str]]) -> int:
    updated = 0
    for item_code, location, carton in keys:
        row = frappe.db.sql(
            """
            SELECT qty_after
            FROM `tabWMS Stock Ledger Entry`
            WHERE company = %s AND item_code = %s AND location = %s AND IFNULL(carton, '') = %s
            ORDER BY posting_datetime DESC, creation DESC
            LIMIT 1
            """,
            (company, item_code, location, carton),
        )
        qty = flt(row[0][0]) if row else 0.0
        filters = {
            "company": company,
            "warehouse": warehouse,
            "item_code": item_code,
            "location": location,
            "carton": carton,
        }
        bal_name = frappe.db.get_value("WMS Stock Balance", filters, "name")
        if qty <= 0:
            if bal_name:
                frappe.delete_doc("WMS Stock Balance", bal_name, ignore_permissions=True, force=True)
                updated += 1
            continue
        if bal_name:
            frappe.db.set_value("WMS Stock Balance", bal_name, "qty", qty, update_modified=False)
        else:
            doc = frappe.get_doc(
                {
                    "doctype": "WMS Stock Balance",
                    "company": company,
                    "warehouse": warehouse,
                    "item_code": item_code,
                    "location": location,
                    "carton": carton,
                    "qty": qty,
                    "reserved_qty": 0,
                    "last_txn_datetime": now_datetime(),
                }
            )
            doc.insert(ignore_permissions=True)
        updated += 1
    return updated


def purge_correction_artifacts(material_request: str, picks: list[dict] | None = None) -> dict:
    """Delete failed correction adjust rows and rebuild affected carton balances."""
    if picks is None:
        picks = load_confirmed_picks().get(material_request) or []
    ste = next((p.get("stock_entry") for p in picks if p.get("stock_entry")), None)
    if not ste:
        ste = frappe.db.get_value(
            "Stock Entry",
            {"custom_material_request": material_request, "docstatus": 1, "remarks": ["like", "%Push from WMS Desktop%"]},
            "name",
        )
    if not ste:
        return {"ok": False, "material_request": material_request, "error": "no_ste"}

    patterns = _correction_remark_patterns(material_request)
    keys: set[tuple[str, str, str]] = set()
    deleted = 0
    for pattern in patterns:
        rows = frappe.db.sql(
            """
            SELECT name, item_code, location, IFNULL(carton, '') AS carton
            FROM `tabWMS Stock Ledger Entry`
            WHERE voucher_name = %s AND event_type = 'Adjust' AND IFNULL(remarks, '') LIKE %s
            """,
            (ste, pattern),
            as_dict=True,
        )
        for row in rows:
            keys.add((cstr(row.item_code), cstr(row.location), cstr(row.carton or "")))
            frappe.delete_doc("WMS Stock Ledger Entry", row.name, ignore_permissions=True, force=True)
            deleted += 1

    se = frappe.get_doc("Stock Entry", ste)
    from_wh = next((r.s_warehouse for r in se.items if r.s_warehouse), None)
    rebuilt = _rebuild_balance_for_keys(se.company, from_wh, keys) if keys else 0
    remaining = _correction_adjust_net_map(ste, material_request)
    remaining_nets = {k: v for k, v in remaining.items() if abs(v) > 1e-9}
    return {
        "ok": not remaining_nets,
        "material_request": material_request,
        "stock_entry": ste,
        "deleted_rows": deleted,
        "balances_rebuilt": rebuilt,
        "remaining_nets": {f"{k[0]}|{k[1]}|{k[2]}": v for k, v in list(remaining_nets.items())[:10]},
    }


@frappe.whitelist(methods=["POST"])
def purge_all_partial_corrections(material_requests=None):
    by_mr = load_confirmed_picks()
    mrs = list(by_mr.keys()) if not material_requests else frappe.parse_json(material_requests)
    results = []
    for mr in mrs:
        ste = by_mr[mr][0].get("stock_entry")
        if verify_correction(ste, aggregate_target(by_mr[mr])).get("ok"):
            results.append({"mr": mr, "skipped": True, "reason": "already_ok"})
            continue
        res = purge_correction_artifacts(mr, by_mr[mr])
        frappe.db.commit()
        results.append(res)
    return {"ok": True, "results": results}

def get_correction_status() -> dict:
    by_mr = load_confirmed_picks()
    ok, partial, pending, blocked = [], [], [], []
    for mr, picks in sorted(by_mr.items()):
        ste = picks[0].get("stock_entry") or frappe.db.get_value(
            "Stock Entry",
            {"custom_material_request": mr, "docstatus": 1, "remarks": ["like", "%Push from WMS Desktop%"]},
            "name",
        )
        target = aggregate_target(picks)
        v = verify_correction(ste, target)
        net = sum(_correction_adjust_net_map(ste, mr).values())
        if v.get("ok"):
            ok.append(mr)
        elif abs(net) > 1e-6:
            partial.append({"mr": mr, "net_adjust": net})
        else:
            pf = preflight_correction(mr, picks)
            if pf.get("ok"):
                pending.append(mr)
            else:
                blocked.append({"mr": mr, "error": pf.get("error"), "blockers": pf.get("blockers", [])[:3]})
    return {"ok_count": len(ok), "partial_count": len(partial), "ready_count": len(pending), "blocked_count": len(blocked), "ok": ok, "partial": partial, "ready": pending, "blocked": blocked}


@frappe.whitelist(methods=["POST"])
def fix_partial_corrections(material_requests=None):
    """Cleanup failed partial correction adjusts, restoring pre-attempt WMS state."""
    by_mr = load_confirmed_picks()
    mrs = list(by_mr.keys()) if not material_requests else frappe.parse_json(material_requests)
    results = []
    for mr in mrs:
        target = aggregate_target(by_mr[mr])
        ste = by_mr[mr][0].get("stock_entry")
        if verify_correction(ste, target).get("ok"):
            results.append({"mr": mr, "skipped": True, "reason": "already_ok"})
            continue
        try:
            res = cleanup_correction_artifacts(mr, by_mr[mr])
            frappe.db.commit()
        except Exception as exc:
            frappe.db.rollback()
            res = {"ok": False, "material_request": mr, "error": cstr(exc)}
        results.append(res)
    return {"ok": True, "results": results}


def _iter_correctable_mrs(by_mr: dict) -> list[str]:
    return sorted(
        [mr for mr in by_mr if mr not in EXCLUDED_MRS and by_mr.get(mr)],
        key=_mr_sort_key,
    )


def _run_correction_batch(*, partial: bool = False, historical: bool = False, dry_run: int = 0) -> dict:
    by_mr = load_confirmed_picks(only_with_ste=True)
    mrs = _iter_correctable_mrs(by_mr)
    results = []
    for mr in mrs:
        picks = by_mr[mr]
        ste = picks[0].get("stock_entry") or _desktop_ste_map().get(mr)
        if ste and verify_correction(ste, aggregate_target(picks)).get("ok"):
            results.append({"ok": True, "skipped": True, "material_request": mr, "stock_entry": ste, "reason": "already_correct"})
            continue
        pf = preflight_correction(mr, picks)
        if pf.get("error") == "qty_total_mismatch":
            results.append({"ok": False, "skipped": True, "material_request": mr, "error": "qty_total_mismatch"})
            continue
        if not partial and not historical and not pf.get("ok"):
            results.append({"ok": False, "skipped": True, "material_request": mr, "error": pf.get("error") or "preflight_failed"})
            continue
        try:
            res = apply_auto_pick_correction_for_mr(
                mr,
                picks,
                dry_run=bool(int(dry_run or 0)),
                partial=partial,
                historical=historical,
            )
        except Exception as exc:
            res = {"ok": False, "material_request": mr, "error": cstr(exc)}
        results.append(res)
        if not bool(int(dry_run or 0)):
            frappe.db.commit()

    corrected = [r for r in results if r.get("ok") and not r.get("skipped")]
    skipped_ok = [r for r in results if r.get("skipped") and r.get("ok")]
    partial_applied = [r for r in results if r.get("ok") and r.get("reason") == "partial_applied"]
    blocked = [r for r in results if r.get("skipped") and not r.get("ok")]
    failed = [r for r in results if not r.get("ok") and not r.get("skipped")]
    return {
        "ok": True,
        "mode": "partial" if partial else ("historical" if historical else "standard"),
        "total_scope": len(mrs),
        "newly_corrected": len([r for r in corrected if r.get("verify", {}).get("ok")]),
        "partial_applied": len(partial_applied),
        "already_correct": len(skipped_ok),
        "skipped_blocked": len(blocked),
        "failed": len(failed),
        "corrected_mrs": [r.get("material_request") for r in corrected if r.get("verify", {}).get("ok")],
        "partial_mrs": [r.get("material_request") for r in partial_applied],
        "failed_mrs": [r.get("material_request") for r in failed],
        "results": results,
    }


@frappe.whitelist(methods=["POST"])
def apply_partial_correctable_batch(dry_run: int = 0) -> dict:
    """Step 1: apply restore/safe deduct lines only."""
    return _run_correction_batch(partial=True, dry_run=dry_run)




@frappe.whitelist(methods=["POST"])
def reapply_historical_correctable_batch(dry_run: int = 0) -> dict:
    """Purge prior partial/historical adjusts then apply one clean historical correction."""
    by_mr = load_confirmed_picks(only_with_ste=True)
    mrs = _iter_correctable_mrs(by_mr)
    results = []
    for mr in mrs:
        picks = by_mr[mr]
        ste = picks[0].get("stock_entry") or _desktop_ste_map().get(mr)
        if verify_correction(ste, aggregate_target(picks)).get("ok"):
            results.append({"ok": True, "skipped": True, "material_request": mr, "reason": "already_correct"})
            continue
        pf = preflight_correction(mr, picks)
        if pf.get("error") == "qty_total_mismatch":
            results.append({"ok": False, "skipped": True, "material_request": mr, "error": "qty_total_mismatch"})
            continue
        purge = purge_correction_artifacts(mr, picks)
        try:
            res = apply_auto_pick_correction_for_mr(mr, picks, dry_run=bool(int(dry_run or 0)), historical=True)
        except Exception as exc:
            res = {"ok": False, "material_request": mr, "error": cstr(exc)}
        res["purged_rows"] = purge.get("deleted_rows", 0)
        results.append(res)
        if not bool(int(dry_run or 0)):
            frappe.db.commit()
    corrected = [r for r in results if r.get("ok") and r.get("verify", {}).get("ok")]
    failed = [r for r in results if not (r.get("skipped") and r.get("ok")) and not r.get("verify", {}).get("ok")]
    return {
        "ok": True,
        "mode": "reapply_historical",
        "total_scope": len(mrs),
        "newly_corrected": len(corrected),
        "already_correct": sum(1 for r in results if r.get("reason") == "already_correct"),
        "failed": len(failed),
        "corrected_mrs": [r.get("material_request") for r in corrected],
        "failed_mrs": [r.get("material_request") for r in failed if not r.get("skipped")][:50],
        "results": results,
    }



MANUAL_STE_REMARK = "WMS historical link {mr} / {ste}"


def link_stock_entry_to_material_request(stock_entry_name: str, material_request: str) -> dict:
    """Link header and all STE lines to Material Request."""
    from printechs_wms.api.stock_entry_material_request import (
        apply_material_request_to_stock_entry,
        has_header_field,
        resolve_mr_item_name,
    )

    if not frappe.db.exists("Stock Entry", stock_entry_name):
        frappe.throw(_("Stock Entry not found: {0}").format(stock_entry_name))
    if not frappe.db.exists("Material Request", material_request):
        frappe.throw(_("Material Request not found: {0}").format(material_request))

    se = frappe.get_doc("Stock Entry", stock_entry_name)
    updated_lines = 0
    if has_header_field():
        frappe.db.set_value("Stock Entry", stock_entry_name, "custom_material_request", material_request, update_modified=True)

    for line in se.items or []:
        mr_item = resolve_mr_item_name(material_request, line.item_code) or ""
        values = {"material_request": material_request}
        if mr_item:
            values["material_request_item"] = mr_item
        frappe.db.set_value("Stock Entry Detail", line.name, values, update_modified=False)
        updated_lines += 1

    frappe.db.commit()
    return {
        "ok": True,
        "stock_entry": stock_entry_name,
        "material_request": material_request,
        "lines_updated": updated_lines,
    }


def load_picks_for_mr(material_request: str, path: Path | None = None) -> list[dict]:
    """Load aggregated desktop picks for one MR."""
    by_mr = load_confirmed_picks(path)
    picks = by_mr.get(material_request) or []
    if picks:
        return picks
    # fallback: scan Transactions for single MR even if only_with_ste filter excluded it
    path = path or _default_picks_path()
    if not path.exists():
        return []
    import openpyxl
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    ws = wb["ConfirmedPicks"] if "ConfirmedPicks" in wb.sheetnames else wb.active
    rows = list(ws.iter_rows(values_only=True))
    header = rows[0]
    idx = {h: i for i, h in enumerate(header)}
    ste = frappe.db.get_value(
        "Stock Entry",
        {"custom_material_request": material_request, "docstatus": 1},
        "name",
    )
    agg: dict[tuple[str, str, str], float] = defaultdict(float)
    if "reference_doc" in idx:
        for row in rows[1:]:
            if not row:
                continue
            if cstr(row[idx.get("reference_doc", "")]).strip() != material_request:
                continue
            if "transaction_type" in idx and cstr(row[idx["transaction_type"]]).strip() not in ("", "Picking"):
                continue
            item = cstr(row[idx["item_code"]]).strip()
            loc = cstr(row[idx["bin_location"]]).strip()
            carton = cstr(row[idx["carton_id"]]).strip()
            if not item or not loc or not carton:
                continue
            agg[(item, loc, carton)] += abs(flt(row[idx["qty_change"]]))
    wb.close()
    return [
        {"item_code": k[0], "location": k[1], "carton": k[2], "qty": q, "stock_entry": ste or ""}
        for k, q in agg.items()
        if q > 0
    ]


def apply_wms_historical_for_ste(material_request: str, stock_entry_name: str, picks: list[dict] | None = None) -> dict:
    """Post WMS Transfer OUT lines on an existing submitted STE from desktop picks."""
    if picks is None:
        picks = load_picks_for_mr(material_request)
    if not picks:
        return {"ok": False, "error": "no_picks", "material_request": material_request, "stock_entry": stock_entry_name}

    se = frappe.get_doc("Stock Entry", stock_entry_name)
    if int(se.docstatus or 0) != 1:
        return {"ok": False, "error": "ste_not_submitted", "stock_entry": stock_entry_name}

    from printechs_wms.api.wms_stock_movement import ledger_applied_for_voucher, apply_line_movement

    if ledger_applied_for_voucher("Stock Entry", stock_entry_name):
        target = aggregate_target(picks)
        verify = verify_correction(stock_entry_name, target)
        if verify.get("ok"):
            return {"ok": True, "skipped": True, "reason": "wms_already_correct", "verify": verify}
        # partial / wrong ledger — continue with remaining deltas below

    ste_qty = {r.item_code: flt(r.qty) for r in se.items if r.s_warehouse}
    pick_qty: dict[str, float] = defaultdict(float)
    for p in picks:
        pick_qty[p["item_code"]] += flt(p["qty"])

    item_gaps = []
    for item, ste_q in ste_qty.items():
        pq = flt(pick_qty.get(item))
        if abs(pq - ste_q) > 1e-6:
            item_gaps.append({"item_code": item, "ste_qty": ste_q, "pick_qty": pq, "diff": pq - ste_q})

    from_wh = next((r.s_warehouse for r in se.items if r.s_warehouse), None)
    remark = MANUAL_STE_REMARK.format(mr=material_request, ste=stock_entry_name)
    lines = sorted(picks, key=lambda p: (p["item_code"], p["location"], p["carton"]))
    applied, ledger_only, errors = [], [], []

    for i, line in enumerate(lines):
        txn_id = _short_txn_id(stock_entry_name, "WMS", i)
        kwargs = dict(
            company=se.company,
            warehouse=from_wh,
            item_code=line["item_code"],
            location=line["location"],
            carton=line["carton"] or None,
            qty_delta=-flt(line["qty"]),
            event_type="Transfer",
            voucher_doctype="Stock Entry",
            voucher_name=stock_entry_name,
            txn_id=txn_id,
            remarks=remark,
        )
        try:
            apply_line_movement(**kwargs)
            applied.append(line)
        except Exception as exc:
            try:
                apply_line_movement(**kwargs, update_balance=False)
                ledger_only.append(line)
            except Exception as exc2:
                errors.append({"line": line, "error": cstr(exc), "ledger_only_error": cstr(exc2)})

    target = aggregate_target(picks)
    verify = verify_correction(stock_entry_name, target)
    try:
        from erpnext.stock.doctype.material_request.material_request import update_completed_and_requested_qty
        update_completed_and_requested_qty(stock_entry_name)
    except Exception:
        pass

    return {
        "ok": verify.get("ok") and not errors,
        "material_request": material_request,
        "stock_entry": stock_entry_name,
        "applied": len(applied),
        "ledger_only": len(ledger_only),
        "errors": errors[:10],
        "item_gaps": item_gaps,
        "pick_total": sum(pick_qty.values()),
        "ste_total": sum(ste_qty.values()),
        "verify": verify,
    }


@frappe.whitelist(methods=["POST"])
def link_ste_and_apply_wms_historical(material_request: str, stock_entry: str) -> dict:
    """Link existing STE to MR and post historical WMS transfer-out from desktop picks."""
    material_request = cstr(material_request).strip()
    stock_entry = cstr(stock_entry).strip()
    link = link_stock_entry_to_material_request(stock_entry, material_request)
    wms = apply_wms_historical_for_ste(material_request, stock_entry)
    return {"ok": wms.get("ok"), "link": link, "wms": wms}

@frappe.whitelist(methods=["POST"])
def apply_historical_correctable_batch(dry_run: int = 0) -> dict:
    """Step 2: apply full desktop correction; ledger-only on blocked deducts."""
    return _run_correction_batch(historical=True, dry_run=dry_run)


def apply_correctable_batch(dry_run: int = 0) -> dict:
    """Apply corrections for submitted-desktop STE MRs that pass preflight."""
    by_mr = load_confirmed_picks(only_with_ste=True)
    mrs = sorted(
        [mr for mr in by_mr if mr not in EXCLUDED_MRS and by_mr.get(mr)],
        key=_mr_sort_key,
    )
    results = []
    for mr in mrs:
        picks = by_mr[mr]
        ste = picks[0].get("stock_entry") or _desktop_ste_map().get(mr)
        if ste and verify_correction(ste, aggregate_target(picks)).get("ok"):
            results.append({"ok": True, "skipped": True, "material_request": mr, "stock_entry": ste})
            continue
        pf = preflight_correction(mr, picks)
        if not pf.get("ok"):
            results.append(
                {
                    "ok": False,
                    "skipped": True,
                    "material_request": mr,
                    "stock_entry": pf.get("stock_entry"),
                    "error": pf.get("error") or "preflight_failed",
                    "blockers": pf.get("blockers", [])[:3],
                }
            )
            continue
        try:
            res = apply_auto_pick_correction_for_mr(mr, picks, dry_run=bool(int(dry_run or 0)))
        except Exception as exc:
            res = {"ok": False, "material_request": mr, "error": cstr(exc)}
        results.append(res)
        if not bool(int(dry_run or 0)):
            frappe.db.commit()

    corrected = [r for r in results if r.get("ok") and not r.get("skipped")]
    skipped_ok = [r for r in results if r.get("skipped") and r.get("ok")]
    blocked = [r for r in results if r.get("skipped") and not r.get("ok")]
    failed = [r for r in results if not r.get("ok") and not r.get("skipped")]
    return {
        "ok": True,
        "total_attempted_scope": len(mrs),
        "newly_corrected": len(corrected),
        "already_correct": len(skipped_ok),
        "blocked": len(blocked),
        "failed": len(failed),
        "corrected_mrs": [r.get("material_request") for r in corrected],
        "blocked_mrs": [r.get("material_request") for r in blocked],
        "failed_mrs": [r.get("material_request") for r in failed],
        "results": results,
    }


def get_correction_status_summary() -> dict:
    """Status for Transactions export vs submitted ERP desktop STE scope."""
    all_by_mr = load_confirmed_picks()
    ste_by_mr = load_confirmed_picks(only_with_ste=True)
    ste_map = _desktop_ste_map()

    corrected, blocked, ready, excluded, no_ste = [], [], [], [], []
    for mr, picks in sorted(ste_by_mr.items()):
        if mr in EXCLUDED_MRS:
            excluded.append(mr)
            continue
        ste = picks[0].get("stock_entry") or ste_map.get(mr)
        if verify_correction(ste, aggregate_target(picks)).get("ok"):
            corrected.append(mr)
            continue
        pf = preflight_correction(mr, picks)
        if pf.get("ok"):
            ready.append(mr)
        else:
            blocked.append({"mr": mr, "error": pf.get("error"), "blockers": pf.get("blockers", [])[:3]})

    for mr in sorted(set(all_by_mr) - set(ste_by_mr)):
        no_ste.append(mr)

    return {
        "source_file": cstr(_default_picks_path()),
        "erp_submitted_mrs": len(ste_map),
        "in_export_with_ste": len(ste_by_mr),
        "corrected_count": len(corrected),
        "ready_count": len(ready),
        "blocked_count": len(blocked),
        "excluded_count": len(excluded),
        "no_erp_ste_count": len(no_ste),
        "remaining_to_fix": len(ready) + len(blocked),
        "corrected": corrected,
        "ready": ready,
        "blocked": blocked,
        "excluded": excluded,
        "no_erp_ste_sample": no_ste[:20],
    }



def apply_ste_scoped_auto_pick_correction_for_mr(
    material_request: str,
    picks: list[dict] | None = None,
    *,
    dry_run: bool = False,
    historical: bool = True,
) -> dict:
    """Correct WMS using desktop picks aligned to submitted STE line qty (partial MR / export excess)."""
    if picks is None:
        picks = load_confirmed_picks().get(material_request) or []
    if not picks:
        frappe.throw(_("No confirmed picks for {0}").format(material_request))
    ste = next((p.get("stock_entry") for p in picks if p.get("stock_entry")), None)
    if not ste:
        ste = frappe.db.get_value(
            "Stock Entry",
            {"custom_material_request": material_request, "docstatus": 1, "remarks": ["like", "%Push from WMS Desktop%"]},
            "name",
        )
    if not ste:
        frappe.throw(_("No submitted desktop Stock Entry for {0}").format(material_request))
    aligned = ste_aligned_picks(picks, ste)
    if not aligned:
        return {"ok": False, "material_request": material_request, "error": "no_ste_aligned_picks"}
    purge_correction_artifacts(material_request, picks)
    res = apply_auto_pick_correction_for_mr(
        material_request,
        aligned,
        dry_run=dry_run,
        historical=historical,
    )
    res["ste_aligned_pick_qty"] = sum(flt(p["qty"]) for p in aligned)
    res["ste_total"] = sum(_ste_item_qty_map(ste).values())
    return res


@frappe.whitelist(methods=["POST"])
def apply_ste_scoped_historical_batch(dry_run: int = 0) -> dict:
    """Historical correction for MRs blocked by full-export qty_total_mismatch."""
    by_mr = load_confirmed_picks(only_with_ste=True)
    mrs = _iter_correctable_mrs(by_mr)
    results = []
    for mr in mrs:
        picks = by_mr[mr]
        ste = picks[0].get("stock_entry") or _desktop_ste_map().get(mr)
        full_target = aggregate_target(picks)
        if verify_correction(ste, full_target).get("ok"):
            results.append({"ok": True, "skipped": True, "material_request": mr, "reason": "already_correct"})
            continue
        pf = preflight_correction(mr, picks)
        if pf.get("error") != "qty_total_mismatch":
            results.append({"ok": False, "skipped": True, "material_request": mr, "error": pf.get("error")})
            continue
        aligned = ste_aligned_picks(picks, ste)
        aligned_target = aggregate_target(aligned)
        erp_total = sum(_transfer_out_map(ste).values())
        if abs(sum(aligned_target.values()) - erp_total) > 1e-6:
            results.append({
                "ok": False,
                "skipped": True,
                "material_request": mr,
                "error": "ste_aligned_still_mismatch",
                "aligned": sum(aligned_target.values()),
                "erp": erp_total,
            })
            continue
        try:
            res = apply_ste_scoped_auto_pick_correction_for_mr(
                mr, picks, dry_run=bool(int(dry_run or 0)), historical=True
            )
        except Exception as exc:
            res = {"ok": False, "material_request": mr, "error": cstr(exc)}
        results.append(res)
        if not bool(int(dry_run or 0)):
            frappe.db.commit()

    corrected = [r for r in results if r.get("ok") and r.get("verify", {}).get("ok")]
    return {
        "ok": True,
        "mode": "ste_scoped_historical",
        "newly_corrected": len(corrected),
        "corrected_mrs": [r.get("material_request") for r in corrected],
        "results": results,
    }


def generate_warehouse_audit_export() -> dict:
    """TSV of location/carton lines warehouse should physically verify (STE-scoped mismatches)."""
    import csv

    doc_dir = Path(__file__).resolve().parents[2] / "doc"
    doc_dir.mkdir(parents=True, exist_ok=True)
    out_path = doc_dir / "warehouse_auto_pick_audit.tsv"

    by_mr = load_confirmed_picks(only_with_ste=True)
    ste_map = _desktop_ste_map()
    rows = []

    for mr in sorted(by_mr):
        if mr in EXCLUDED_MRS:
            continue
        picks = by_mr[mr]
        ste = picks[0].get("stock_entry") or ste_map.get(mr)
        if not ste:
            continue
        pf = preflight_correction(mr, picks)
        if pf.get("error") == "qty_total_mismatch":
            target = ste_aligned_target(picks, ste)
        else:
            target = aggregate_target(picks)
        v = verify_correction(ste, target)
        if v.get("ok"):
            continue
        erp_out = _transfer_out_map(ste)
        for m in v.get("mismatches") or []:
            key = (m["item_code"], m["location"], m["carton"])
            rows.append(
                {
                    "material_request": mr,
                    "stock_entry": ste,
                    "item_code": m["item_code"],
                    "bin_location": m["location"],
                    "carton_id": m["carton"],
                    "desktop_qty_expected": m["target_out"],
                    "erp_wms_net_out": -flt(m["net_ledger"]),
                    "erp_auto_pick_out": flt(erp_out.get(key)),
                    "qty_gap": flt(m["target_out"]) + flt(m["net_ledger"]),
                }
            )
        # include all mismatches not truncated
        if v.get("mismatch_count", 0) > len(v.get("mismatches") or []):
            full = verify_correction(stock_entry_name=ste, target=target)
            # re-run without slice - fix verify to return all or query here
            pass

    # full mismatch list (verify truncates to 20)
    full_rows = []
    for mr in sorted(by_mr):
        if mr in EXCLUDED_MRS:
            continue
        picks = by_mr[mr]
        ste = picks[0].get("stock_entry") or ste_map.get(mr)
        if not ste:
            continue
        pf = preflight_correction(mr, picks)
        target = ste_aligned_target(picks, ste) if pf.get("error") == "qty_total_mismatch" else aggregate_target(picks)
        erp_out = _transfer_out_map(ste)
        for key, tqty in target.items():
            net = flt(
                frappe.db.sql(
                    """
                    SELECT SUM(qty_change)
                    FROM `tabWMS Stock Ledger Entry`
                    WHERE voucher_doctype = 'Stock Entry'
                      AND voucher_name = %s
                      AND item_code = %s
                      AND location = %s
                      AND IFNULL(carton, '') = %s
                    """,
                    (ste, key[0], key[1], key[2]),
                )[0][0]
            )
            if abs(net + flt(tqty)) <= 1e-6:
                continue
            full_rows.append(
                {
                    "material_request": mr,
                    "stock_entry": ste,
                    "item_code": key[0],
                    "bin_location": key[1],
                    "carton_id": key[2],
                    "desktop_qty_expected": flt(tqty),
                    "erp_wms_net_out": -net,
                    "erp_auto_pick_out": flt(erp_out.get(key)),
                    "qty_gap": flt(tqty) + net,
                }
            )

    fields = [
        "material_request", "stock_entry", "item_code", "bin_location", "carton_id",
        "desktop_qty_expected", "erp_wms_net_out", "erp_auto_pick_out", "qty_gap",
    ]
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, delimiter="\t")
        w.writeheader()
        w.writerows(full_rows)

    cartons = {(r["bin_location"], r["carton_id"]) for r in full_rows}
    return {
        "ok": True,
        "file": str(out_path),
        "audit_lines": len(full_rows),
        "unique_location_cartons": len(cartons),
        "unique_mrs": len({r["material_request"] for r in full_rows}),
    }


def generate_blocker_report() -> dict:
    """Export TSV of blocked MR correction lines for warehouse review."""
    import csv

    out_dir = Path("/home/erpnext/frappe-bench/sites/moosa.live/private")
    out_detail = out_dir / "auto_pick_blocker_report.tsv"
    out_summary = out_dir / "auto_pick_blocker_mr_summary.tsv"

    by_mr = load_confirmed_picks(only_with_ste=True)
    ste_map = _desktop_ste_map()
    detail_rows = []
    summary_rows = []

    for mr in sorted(by_mr):
        picks = by_mr[mr]
        ste = picks[0].get("stock_entry") or ste_map.get(mr)
        if mr in EXCLUDED_MRS:
            summary_rows.append({
                "material_request": mr,
                "stock_entry": ste or "",
                "status": "EXCLUDED",
                "error": "qty_total_mismatch",
                "blocker_lines": 0,
                "desktop_carton_lines": len(picks),
            })
            continue

        target = aggregate_target(picks)
        if verify_correction(ste, target).get("ok"):
            summary_rows.append({
                "material_request": mr,
                "stock_entry": ste,
                "status": "CORRECTED",
                "error": "",
                "blocker_lines": 0,
                "desktop_carton_lines": len(picks),
            })
            continue

        pf = preflight_correction(mr, picks)
        if pf.get("ok"):
            summary_rows.append({
                "material_request": mr,
                "stock_entry": ste,
                "status": "READY",
                "error": "",
                "blocker_lines": 0,
                "desktop_carton_lines": len(picks),
            })
            continue

        erp_out = _transfer_out_map(ste)
        se = frappe.get_doc("Stock Entry", ste)
        from_wh = next((r.s_warehouse for r in se.items if r.s_warehouse), None)
        blockers = pf.get("blockers") or []
        blocker_keys = {(b["item_code"], b["location"], b["carton"]) for b in blockers}

        deltas = []
        for key in set(target) | set(erp_out):
            delta = flt(target.get(key)) - flt(erp_out.get(key))
            if abs(delta) <= 1e-9:
                continue
            deltas.append((key, -delta))

        for key, qty_delta in sorted(deltas, key=lambda x: (-1 if x[1] > 0 else 1, x[0])):
            bal = flt(
                frappe.db.get_value(
                    "WMS Stock Balance",
                    {
                        "item_code": key[0],
                        "location": key[1],
                        "carton": key[2],
                        "warehouse": from_wh,
                    },
                    "qty",
                )
            )
            is_blocker = key in blocker_keys
            b = next(
                (x for x in blockers if x["item_code"] == key[0] and x["location"] == key[1] and x["carton"] == key[2]),
                {},
            )
            detail_rows.append({
                "material_request": mr,
                "stock_entry": ste,
                "item_code": key[0],
                "bin_location": key[1],
                "carton_id": key[2],
                "desktop_qty": flt(target.get(key)),
                "erp_auto_pick_out": flt(erp_out.get(key)),
                "correction_qty_delta": qty_delta,
                "wms_balance_now": bal,
                "balance_after_correction": flt(b.get("balance_after", bal + qty_delta)),
                "is_blocker": "Y" if is_blocker else "N",
                "blocker_reason": "insufficient_balance" if is_blocker else "",
            })

        err = pf.get("error") or "preflight_failed"
        summary_rows.append({
            "material_request": mr,
            "stock_entry": ste,
            "status": "BLOCKED" if err == "preflight_failed" else "DATA_ERROR",
            "error": err,
            "blocker_lines": len(blockers),
            "desktop_carton_lines": len(picks),
        })

    detail_fields = [
        "material_request", "stock_entry", "item_code", "bin_location", "carton_id",
        "desktop_qty", "erp_auto_pick_out", "correction_qty_delta", "wms_balance_now",
        "balance_after_correction", "is_blocker", "blocker_reason",
    ]
    summary_fields = [
        "material_request", "stock_entry", "status", "error", "blocker_lines", "desktop_carton_lines",
    ]

    with out_detail.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=detail_fields, delimiter="\t")
        w.writeheader()
        w.writerows(detail_rows)

    with out_summary.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=summary_fields, delimiter="\t")
        w.writeheader()
        w.writerows(summary_rows)

    return {
        "ok": True,
        "detail_file": str(out_detail),
        "summary_file": str(out_summary),
        "detail_lines": len(detail_rows),
        "blocker_lines": sum(1 for r in detail_rows if r["is_blocker"] == "Y"),
        "blocked_mrs": sum(1 for r in summary_rows if r["status"] == "BLOCKED"),
        "corrected_mrs": sum(1 for r in summary_rows if r["status"] == "CORRECTED"),
        "ready_mrs": sum(1 for r in summary_rows if r["status"] == "READY"),
        "excluded_mrs": sum(1 for r in summary_rows if r["status"] == "EXCLUDED"),
    }
