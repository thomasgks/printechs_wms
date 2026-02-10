import frappe
import re
from frappe.utils import now

def _to_key(s):
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_")

def update_custom_wms_modified(doc, method=None):
    doc.custom_wms_modified = now()

@frappe.whitelist()
def get_items_compact(
    modified_after=None,
    limit=200,
    offset=0,
    filters=None,
    fields=None,
    flatten_attributes=1,
    flatten_barcodes=1
):
    limit = int(limit or 200)
    offset = int(offset or 0)

    # -------------------------
    # Base filters (MANDATORY)
    # -------------------------
    erp_filters = [
        ["Item", "has_variants", "=", 0],     # ? exclude templates
        ["Item", "is_stock_item", "=", 1],
        ["Item", "disabled", "=", 0],
    ]

    if modified_after:
        erp_filters.append(["Item", "modified", ">", modified_after])

    # -------------------------
    # Custom record filters
    # -------------------------
    if filters:
        if isinstance(filters, str):
            filters = frappe.parse_json(filters)

        for key, value in filters.items():
            erp_filters.append(["Item", key, "=", value])

    # -------------------------
    # Fetch items
    # -------------------------
    rows = frappe.get_all(
        "Item",
        filters=erp_filters,
        fields=["name", "modified"],
        order_by="modified asc",
        limit_start=offset,
        limit_page_length=limit
    )

    items = []
    max_modified = None

    for r in rows:
        doc = frappe.get_doc("Item", r.name)

        data = {
            "item_code": doc.item_code,
            "item_name": doc.item_name,
            "item_group": doc.item_group,
            "brand": doc.brand,
            "stock_uom": doc.stock_uom,
            "modified": doc.modified,
            "is_stock": doc.is_stock_item,
            "attributes": {},
            "barcodes": []
        }

        # Attributes
        for a in doc.attributes or []:
            data["attributes"][a.attribute] = a.attribute_value

        # Barcodes
        for b in doc.barcodes or []:
            if b.barcode:
                data["barcodes"].append(b.barcode)

        # Flatten attributes
        if int(flatten_attributes) == 1:
            for k, v in data["attributes"].items():
                data[_to_key(k)] = v
            data.pop("attributes", None)

        # Flatten barcode
        if int(flatten_barcodes) == 1:
            data["barcode"] = data["barcodes"][0] if data["barcodes"] else None

        items.append(data)

        if not max_modified or doc.modified > max_modified:
            max_modified = doc.modified

    # -------------------------
    # Field-level filtering
    # -------------------------
    if fields:
        if isinstance(fields, str):
            fields = frappe.parse_json(fields)

        items = [
            {k: item.get(k) for k in fields if k in item}
            for item in items
        ]

    return {
        "items": items,
        "limit": limit,
        "offset": offset,
        "has_more": len(items) == limit,
        "max_modified": max_modified
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
