from __future__ import annotations

import frappe


def _truthy(v) -> bool:
    if v is None:
        return False
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    return s in {"1", "true", "yes", "y", "on"}


def _warehouse_code(warehouse_name: str | None) -> str | None:
    """
    Return Warehouse.code (store code) if exists and filled, else warehouse name.
    """
    if not warehouse_name:
        return None

    if frappe.db.has_column("Warehouse", "code"):
        code = frappe.db.get_value("Warehouse", warehouse_name, "code")
        if code:
            return code

    return warehouse_name


@frappe.whitelist()
def get_material_transfer_requests(
    from_warehouse: str = "Main Warehouse - MAATC",
    material_request_type: str = "Material Transfer",
    status: str | None = None,          # optionally pass status=Draft
    docstatus: str | int = 0,           # 0=Draft, 1=Submitted
    include_items: str | int = 1,
    limit: str | int = 200,
):
    """
    WMS API: Pull Material Requests (Material Transfer) for WMS picking/transfer

    Filters (default):
      - set_from_warehouse = from_warehouse
      - material_request_type = Material Transfer
      - docstatus = 0 (Draft)  [because your workflow shows "Pending" but DB is Draft]
      - optionally status filter (e.g. Draft)

    Output:
      - from_warehouse_code and to_warehouse_code are Warehouse.code (store code)
      - items included when include_items=1
    """

    include_items_bool = _truthy(include_items)
    limit_n = int(limit) if str(limit).strip().isdigit() else 200
    ds = int(docstatus) if str(docstatus).strip().isdigit() else 0

    # If you created this custom field in Material Request (fetch from Warehouse.code),
    # we'll use it as first priority; otherwise we compute from Warehouse.code.
    has_from_code_field = frappe.db.has_column("Material Request", "from_warehouse_code")

    # -----------------------------
    # Filters
    # -----------------------------
    filters = {
        "docstatus": ds,
        "material_request_type": material_request_type,
        "set_from_warehouse": from_warehouse,
    }
    if status:
        filters["status"] = status

    # -----------------------------
    # Header fields
    # -----------------------------
    header_fields = [
        "name",
        "company",
        "transaction_date",
        "schedule_date",
        "status",
        "owner",
        "set_from_warehouse",
        "set_warehouse",
        "material_request_type",
        "docstatus",
        "modified",
    ]
    if has_from_code_field:
        header_fields.append("from_warehouse_code")

    mr_list = frappe.get_all(
        "Material Request",
        filters=filters,
        fields=header_fields,
        order_by="modified desc",
        limit_page_length=limit_n,
    )

    if not mr_list:
        return {"ok": True, "count": 0, "filters_used": filters, "data": []}

    # -----------------------------
    # Preload warehouse codes (single query)
    # -----------------------------
    wh_names = set()
    for mr in mr_list:
        if mr.get("set_from_warehouse"):
            wh_names.add(mr["set_from_warehouse"])
        if mr.get("set_warehouse"):
            wh_names.add(mr["set_warehouse"])

    wh_map: dict[str, str] = {}
    if wh_names:
        wh_fields = ["name"]
        if frappe.db.has_column("Warehouse", "code"):
            wh_fields.append("code")

        wh_rows = frappe.get_all(
            "Warehouse",
            filters={"name": ["in", list(wh_names)]},
            fields=wh_fields,
        )

        for r in wh_rows:
            wh_map[r["name"]] = r.get("code") or r["name"]

    # -----------------------------
    # Build response
    # -----------------------------
    data = []

    for mr in mr_list:
        from_wh_name = mr.get("set_from_warehouse")
        to_wh_name = mr.get("set_warehouse")

        # from_warehouse_code priority:
        # 1) MR.from_warehouse_code (if exists and filled)
        # 2) Warehouse.code from preload map
        # 3) direct lookup fallback
        from_code = mr.get("from_warehouse_code") if has_from_code_field else None
        if not from_code:
            from_code = wh_map.get(from_wh_name) or _warehouse_code(from_wh_name)

        # to_warehouse_code from preload map or fallback
        to_code = wh_map.get(to_wh_name) or _warehouse_code(to_wh_name)

        items_out = []
        total_requested_qty = 0.0
        items_count = 0

        if include_items_bool:
            doc = frappe.get_doc("Material Request", mr["name"])
            for it in (doc.items or []):
                qty = float(it.qty or 0)
                total_requested_qty += qty
                items_count += 1

                # Target warehouse can be per-line (it.warehouse) or header set_warehouse
                line_target_wh = it.warehouse or to_wh_name
                line_target_code = wh_map.get(line_target_wh) or _warehouse_code(line_target_wh)

                items_out.append({
                    "item_code": it.item_code,
                    "item_name": it.item_name,
                    "uom": it.uom,
                    "requested_qty": qty,
                    "required_by": it.schedule_date or mr.get("schedule_date"),

                    "target_warehouse_name": line_target_wh,
                    "target_warehouse_code": line_target_code,
                })
        else:
            items_count = frappe.db.count("Material Request Item", {"parent": mr["name"]})

        data.append({
            "material_request_no": mr["name"],
            "docstatus": mr.get("docstatus"),
            "status": mr.get("status"),
            "material_request_type": mr.get("material_request_type"),

            "company": mr.get("company"),
            "requested_by": mr.get("owner"),
            "requested_date": mr.get("transaction_date"),
            "required_date": mr.get("schedule_date"),

            "from_warehouse_name": from_wh_name,
            "from_warehouse_code": from_code,

            "to_warehouse_name": to_wh_name,
            "to_warehouse_code": to_code,

            "total_requested_qty": total_requested_qty,
            "items_count": items_count,
            "items": items_out if include_items_bool else None,
        })

    return {"ok": True, "count": len(data), "filters_used": filters, "data": data}
