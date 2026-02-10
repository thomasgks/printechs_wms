import frappe
import re
from frappe.utils import now
from frappe.utils import cint


import frappe
from frappe.utils import cint


def _ensure_dict(value):
    if not value:
        return {}
    return frappe.parse_json(value) if isinstance(value, str) else value


def _ensure_list(value):
    if not value:
        return []
    return frappe.parse_json(value) if isinstance(value, str) else value


def _to_key(s: str) -> str:
    if not s:
        return ""
    s = s.strip().lower()
    out = []
    prev_us = False
    for ch in s:
        if ch.isalnum():
            out.append(ch)
            prev_us = False
        else:
            if not prev_us:
                out.append("_")
                prev_us = True
    key = "".join(out).strip("_")
    while "__" in key:
        key = key.replace("__", "_")
    return key


def _get_attribute_name_map(attribute_keys: list[str]) -> dict[str, str]:
    if not attribute_keys:
        return {}

    rows = frappe.get_all("Item Attribute", fields=["name"])
    all_names = [r["name"] for r in rows]
    lower_map = {n.strip().lower(): n for n in all_names}

    result = {}
    for k in attribute_keys:
        lk = (k or "").strip().lower()
        result[k] = lower_map.get(lk, k)
    return result


def _select_parents_by_attribute_filter(attribute_name: str, op: str, val):
    op = (op or "").strip().lower()
    allowed = {"=", "!=", ">", ">=", "<", "<=", "like", "not like", "in", "not in"}
    if op not in allowed:
        frappe.throw(f"Unsupported operator in attribute_filters: {op}")

    numeric_ops = {">", ">=", "<", "<="}

    if op in {"in", "not in"}:
        if not isinstance(val, (list, tuple)) or not val:
            frappe.throw(f"attribute_filters for '{attribute_name}' with '{op}' must be a list")
        placeholders = ", ".join(["%s"] * len(val))
        sql = f"""
            SELECT parent
            FROM `tabItem Variant Attribute`
            WHERE LOWER(attribute) = LOWER(%s)
              AND attribute_value {op} ({placeholders})
        """
        params = [attribute_name, *val]
    else:
        if op in numeric_ops:
            sql = f"""
                SELECT parent
                FROM `tabItem Variant Attribute`
                WHERE LOWER(attribute) = LOWER(%s)
                  AND CAST(attribute_value AS UNSIGNED) {op} %s
            """
            params = [attribute_name, cint(val)]
        else:
            sql = f"""
                SELECT parent
                FROM `tabItem Variant Attribute`
                WHERE LOWER(attribute) = LOWER(%s)
                  AND attribute_value {op} %s
            """
            params = [attribute_name, val]

    rows = frappe.db.sql(sql, params)
    return {r[0] for r in rows}


def _apply_attribute_filters_to_item_filters(item_filters: dict, attribute_filters: dict):
    if not attribute_filters:
        return

    keys = list(attribute_filters.keys())
    name_map = _get_attribute_name_map(keys)

    matched_sets = []
    for req_key, cond in attribute_filters.items():
        if not isinstance(cond, (list, tuple)) or len(cond) != 2:
            frappe.throw(f"Invalid attribute_filters for '{req_key}'. Use: [operator, value]")

        op, val = cond[0], cond[1]
        attr_name = name_map.get(req_key, req_key)

        matched = _select_parents_by_attribute_filter(attr_name, op, val)
        matched_sets.append(matched)

    matched_all = set.intersection(*matched_sets) if matched_sets else set()
    if not matched_all:
        item_filters["name"] = ["in", ["__NO_MATCH__"]]
        return

    item_filters["name"] = ["in", list(matched_all)]


def _fetch_attributes_for_items(item_names: list[str]) -> dict[str, dict[str, str]]:
    if not item_names:
        return {}

    placeholders = ", ".join(["%s"] * len(item_names))
    sql = f"""
        SELECT parent, attribute, attribute_value
        FROM `tabItem Variant Attribute`
        WHERE parent IN ({placeholders})
    """
    rows = frappe.db.sql(sql, item_names, as_dict=True)

    out = {}
    for r in rows:
        out.setdefault(r["parent"], {})
        out[r["parent"]][r["attribute"]] = r["attribute_value"]
    return out


def _fetch_barcodes_for_items(item_names: list[str]) -> dict[str, list[str]]:
    """
    ERPNext standard: Item Barcode child table
    - Parent is Item
    - Field is barcode
    """
    if not item_names:
        return {}

    # Check if the doctype exists (some ERPs may not have it)
    if not frappe.db.table_exists("Item Barcode"):
        return {}

    placeholders = ", ".join(["%s"] * len(item_names))
    sql = f"""
        SELECT parent, barcode
        FROM `tabItem Barcode`
        WHERE parent IN ({placeholders})
    """
    rows = frappe.db.sql(sql, item_names, as_dict=True)

    out = {}
    for r in rows:
        out.setdefault(r["parent"], [])
        if r.get("barcode"):
            out[r["parent"]].append(r["barcode"])
    return out


@frappe.whitelist()
def get_items_compact(
    filters=None,
    fields=None,
    limit=100,
    offset=0,
    custom_wms_modified_after=None,
    attribute_filters=None,
    flatten_attributes=1
):
    filters = _ensure_dict(filters)
    fields = _ensure_list(fields)
    attribute_filters = _ensure_dict(attribute_filters)

    limit = cint(limit) or 100
    offset = cint(offset) or 0
    flatten_attributes = cint(flatten_attributes) if str(flatten_attributes).strip() != "" else 1

    # Default returned fields if not requested
    if not fields:
        fields = [
            "item_code",
            "item_name",
            "item_group",
            "brand",
            "stock_uom",
            "is_stock",
            "disabled",
            "custom_wms_modified",
        ]

    # Computed output keys (not real Item columns)
    computed_keys = {"year", "season", "color", "size"}
    wants_is_stock = any(isinstance(f, str) and f.lower() == "is_stock" for f in fields)

    # Barcode is NOT a column in your tabItem; handle separately via Item Barcode child table.
    wants_barcode = any(isinstance(f, str) and f.lower() == "barcode" for f in fields)

    # Build DB fields list (only real columns)
    db_fields = []
    for f in fields:
        if not isinstance(f, str):
            continue
        lf = f.lower()
        if lf in computed_keys:
            continue
        if lf == "is_stock":
            continue
        if lf == "barcode":
            continue
        db_fields.append(f)

    # Always include name and cursor field for internal joins / cursor calc
    if "name" not in db_fields:
        db_fields.append("name")
    if "custom_wms_modified" not in db_fields:
        db_fields.append("custom_wms_modified")
    if "disabled" not in db_fields:
        db_fields.append("disabled")

    # is_stock mapping needs is_stock_item from Item
    if wants_is_stock and "is_stock_item" not in db_fields:
        db_fields.append("is_stock_item")

    # incremental cursor
    if custom_wms_modified_after:
        filters["custom_wms_modified"] = (">", custom_wms_modified_after)

    # attribute filters (variant attributes)
    _apply_attribute_filters_to_item_filters(filters, attribute_filters)

    # Fetch page (+1 to detect has_more)
    rows = frappe.get_all(
        "Item",
        filters=filters,
        fields=db_fields,
        order_by="custom_wms_modified asc",
        limit_start=offset,
        limit_page_length=limit + 1
    )

    has_more = len(rows) > limit
    if has_more:
        rows = rows[:limit]

    max_custom_wms_modified = rows[-1].get("custom_wms_modified") if rows else None

    item_names = [r.get("name") for r in rows if r.get("name")]

    # Fetch variant attributes if needed
    attrs_by_item = {}
    if flatten_attributes == 1 and (any(f.lower() in computed_keys for f in fields if isinstance(f, str)) or attribute_filters):
        attrs_by_item = _fetch_attributes_for_items(item_names)

    # Fetch barcodes if requested
    barcodes_by_item = {}
    if wants_barcode:
        barcodes_by_item = _fetch_barcodes_for_items(item_names)

    out_items = []
    for r in rows:
        data = dict(r)

        # Map is_stock
        if wants_is_stock:
            data["is_stock"] = 1 if data.get("is_stock_item") else 0

        # Flatten attributes
        if flatten_attributes == 1:
            attrs = attrs_by_item.get(data.get("name"), {})
            for ak, av in (attrs or {}).items():
                data[_to_key(ak)] = av

        # Barcode: choose first barcode (or return list if you prefer)
        if wants_barcode:
            bcs = barcodes_by_item.get(data.get("name"), [])
            data["barcode"] = bcs[0] if bcs else None

        # Remove internal fields if not requested
        if "name" not in fields:
            data.pop("name", None)
        if "is_stock_item" not in fields:
            data.pop("is_stock_item", None)

        out_items.append(data)

    return {
        "items": out_items,
        "limit": limit,
        "offset": offset,
        "has_more": has_more,
        "max_custom_wms_modified": max_custom_wms_modified
    }




@frappe.whitelist()
def get_item_compact(item_code=None, fields=None, attribute_fields=None, flatten_attributes=0, flatten_barcodes=0):
    if not item_code:
        frappe.throw("item_code is required")

    item = frappe.get_doc("Item", item_code)

    result = {
        "item_code": item.item_code,
        "item_name": item.item_name,
        "item_group": item.item_group,
        "brand": item.brand,
        "stock_uom": item.stock_uom,
        "modified": item.modified,
        "attributes": {},
        "barcodes": []
    }

    for a in (item.attributes or []):
        result["attributes"][a.attribute] = a.attribute_value

    for b in (item.barcodes or []):
        if b.barcode:
            result["barcodes"].append(b.barcode)

    if attribute_fields:
        attrs = attribute_fields
        if isinstance(attrs, str):
            attrs = frappe.parse_json(attrs)
        result["attributes"] = {k: v for k, v in result["attributes"].items() if k in attrs}

    if int(flatten_attributes) == 1:
        for k, v in (result.get("attributes") or {}).items():
            result[_to_key(k)] = v
        result.pop("attributes", None)

    if int(flatten_barcodes) == 1:
        result["barcode"] = result["barcodes"][0] if result["barcodes"] else None

    if fields:
        req = fields
        if isinstance(req, str):
            req = frappe.parse_json(req)
        return {k: result[k] for k in req if k in result}

    return result
# -*- coding: utf-8 -*-



def update_custom_wms_modified(doc, method=None):
    """
    Hook target referenced from hooks.py

    Purpose:
      - Touch custom_wms_modified timestamp (if field exists)
      - Never block saving Item
    """
    try:
        if not doc:
            return

        # Only update if field exists
        if hasattr(doc, "meta") and doc.meta and doc.meta.has_field("custom_wms_modified"):
            doc.custom_wms_modified = now()

    except Exception:
        # Never break Item save due to sync timestamps
        pass
