"""Setup Warehouse User role and User custom fields for WMS sync."""

import frappe


def setup_warehouse_user_master():
	_ensure_warehouse_user_role()
	_ensure_user_custom_fields()
	try:
		from printechs_wms.api.warehouse import setup_warehouse_wms_fields

		setup_warehouse_wms_fields()
	except Exception:
		pass


def _ensure_warehouse_user_role():
	if frappe.db.exists("Role", "Warehouse User"):
		return

	doc = frappe.get_doc(
		{
			"doctype": "Role",
			"role_name": "Warehouse User",
			"desk_access": 1,
		}
	)
	doc.insert(ignore_permissions=True)


def _ensure_user_custom_fields():
	from frappe.custom.doctype.custom_field.custom_field import create_custom_field

	fields = [
		{
			"fieldname": "wms_user_section",
			"label": "WMS",
			"fieldtype": "Section Break",
			"insert_after": "roles",
		},
		{
			"fieldname": "custom_is_warehouse_user",
			"label": "Is Warehouse User",
			"fieldtype": "Check",
			"insert_after": "wms_user_section",
			"description": "Include this user in WMS user sync when Warehouse User role is assigned.",
		},
		{
			"fieldname": "custom_wms_modified",
			"label": "WMS Modified",
			"fieldtype": "Datetime",
			"insert_after": "custom_is_warehouse_user",
			"read_only": 1,
			"no_copy": 1,
		},
	]

	for field in fields:
		fieldname = field["fieldname"]
		if frappe.db.exists("Custom Field", f"User-{fieldname}"):
			continue
		create_custom_field("User", field, ignore_validate=True)
