# apps/printechs_wms/printechs_wms/api/location.py
import frappe
from frappe import _

DOCTYPE = "WMS Bin Location"

DEFAULT_FIELDS = [
    "name", "location_id", "location_name", "status", "company",
    "zone", "aisle", "rack", "level", "bin",
    "erp_warehouse", "location_type", "priority",
    "is_locked", "allow_mixed_items", "allow_mixed_cartons",
    "modified", "creation"
]

def _to_int(val, default):
    try:
        return int(val)
    except Exception:
        return default

def _to_bool01(val):
    """Return 1/0/None based on input."""
    if val is None or val == "":
        return None
    if isinstance(val, bool):
        return 1 if val else 0
    v = str(val).strip().lower()
    if v in ("1", "true", "yes", "y", "on"):
        return 1
    if v in ("0", "false", "no", "n", "off"):
        return 0
    return None

def _safe_fields(fields):
    """Allow only known fields to prevent injection/invalid field errors."""
    allowed = set(DEFAULT_FIELDS)
    if not fields:
        return DEFAULT_FIELDS
    out = []
    for f in str(fields).split(","):
        f = f.strip()
        if f and f in allowed:
            out.append(f)
    return out or DEFAULT_FIELDS

def _build_filters(company=None, status=None, zone=None, aisle=None, rack=None,
                   erp_warehouse=None, location_type=None, is_locked=None):
    filters = {}
    if company:
        filters["company"] = company
    if status:
        filters["status"] = status
    if zone:
        filters["zone"] = zone
    if aisle:
        filters["aisle"] = aisle
    if rack:
        filters["rack"] = rack
    if erp_warehouse:
        filters["erp_warehouse"] = erp_warehouse
    if location_type:
        filters["location_type"] = location_type
    if is_locked is not None:
        filters["is_locked"] = is_locked
    return filters

def _count_with_search_sql(filters: dict, search: str) -> int:
    """
    Count rows with AND filters + search OR over multiple columns.
    Compatible with older Frappe versions.
    """
    where = ["1=1"]
    params = {}

    # AND filters
    for k, v in (filters or {}).items():
        where.append(f"`{k}` = %({k})s")
        params[k] = v

    # OR search across fields
    s = (search or "").strip()
    if s:
        params["like"] = f"%{s}%"
        where.append("("
            "`location_id` LIKE %(like)s OR "
            "`location_name` LIKE %(like)s OR "
            "`zone` LIKE %(like)s OR "
            "`aisle` LIKE %(like)s OR "
            "`rack` LIKE %(like)s OR "
            "`bin` LIKE %(like)s"
        ")")

    sql = f"SELECT COUNT(*) AS cnt FROM `tab{DOCTYPE}` WHERE " + " AND ".join(where)
    return int(frappe.db.sql(sql, params)[0][0])

@frappe.whitelist(allow_guest=False)
def list_locations(
    company=None,
    status="Active",
    zone=None,
    aisle=None,
    rack=None,
    erp_warehouse=None,
    location_type=None,
    is_locked=None,
    search=None,
    page=1,
    page_size=50,
    order_by="location_id asc",
    fields=None,
    include_total=1
):
    """
    GET /api/method/printechs_wms.api.location.list_locations

    Params:
      company (required for most setups)
      status (default Active)
      search (optional)
      page, page_size
      fields (optional comma-separated from DEFAULT_FIELDS)
      include_total (1/0)
    """

    page = max(_to_int(page, 1), 1)
    page_size = min(max(_to_int(page_size, 50), 1), 500)
    start = (page - 1) * page_size

    include_total = _to_bool01(include_total)
    is_locked_val = _to_bool01(is_locked)

    field_list = _safe_fields(fields)

    filters = _build_filters(
        company=company,
        status=status if status else None,
        zone=zone,
        aisle=aisle,
        rack=rack,
        erp_warehouse=erp_warehouse,
        location_type=location_type,
        is_locked=is_locked_val
    )

    # Data fetch using get_all with or_filters for search
    or_filters = None
    s = (search or "").strip()
    if s:
        like = f"%{s}%"
        or_filters = [
            [DOCTYPE, "location_id", "like", like],
            [DOCTYPE, "location_name", "like", like],
            [DOCTYPE, "zone", "like", like],
            [DOCTYPE, "aisle", "like", like],
            [DOCTYPE, "rack", "like", like],
            [DOCTYPE, "bin", "like", like],
        ]

    data = frappe.get_all(
        DOCTYPE,
        filters=filters,
        or_filters=or_filters,
        fields=field_list,
        order_by=order_by,
        start=start,
        page_length=page_size
    )

    total = None
    if include_total == 1:
        # IMPORTANT: NEVER pass or_filters to frappe.db.count in your version.
        if s:
            total = _count_with_search_sql(filters, s)
        else:
            total = frappe.db.count(DOCTYPE, filters=filters)

    return {
        "ok": True,
        "page": page,
        "page_size": page_size,
        "total": total,
        "data": data
    }

@frappe.whitelist(allow_guest=False)
def get_location(location_id=None, name=None):
    """
    GET /api/method/printechs_wms.api.location.get_location?location_id=A1-R01-L4-B1
    or ?name=<docname>
    """
    if not location_id and not name:
        frappe.throw(_("Provide location_id or name"))

    docname = name
    if not docname and location_id:
        docname = frappe.db.get_value(DOCTYPE, {"location_id": location_id}, "name")

    if not docname:
        frappe.throw(_("Location not found"))

    doc = frappe.get_doc(DOCTYPE, docname)
    return {"ok": True, "data": doc.as_dict()}

@frappe.whitelist(allow_guest=False)
def ping():
    """Simple health check to confirm latest code is loaded."""
    return {"ok": True, "message": "printechs_wms location api ping ok"}
