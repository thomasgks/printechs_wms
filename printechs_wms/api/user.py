# Copyright (c) 2026, Printechs and contributors
# License: MIT. See license.txt

"""Compact warehouse user pull API for WMS clients."""

from __future__ import annotations

import frappe
from frappe.utils import cint, now

WMS_WAREHOUSE_USER_ROLE = "Warehouse User"

DEFAULT_FIELDS = [
	"user_code",
	"name",
	"role",
	"active",
	"email",
	"mobile_no",
	"updated_on",
]

ADMIN_WMS_ROLES = frozenset(
	{
		"System Manager",
		"Stock Manager",
		"Warehouse Manager",
	}
)

OPERATOR_WMS_ROLES = frozenset(
	{
		WMS_WAREHOUSE_USER_ROLE,
		"Stock User",
		"Item Manager",
	}
)


def _ensure_dict(value):
	if not value:
		return {}
	return frappe.parse_json(value) if isinstance(value, str) else value


def _ensure_list(value):
	if not value:
		return []
	return frappe.parse_json(value) if isinstance(value, str) else value


def _user_has_wms_field(fieldname: str) -> bool:
	return frappe.get_meta("User").has_field(fieldname)


def _warehouse_user_role_holders() -> set[str]:
	return set(
		frappe.get_all(
			"Has Role",
			filters={"parenttype": "User", "role": WMS_WAREHOUSE_USER_ROLE},
			pluck="parent",
		)
	)


def _map_wms_role(erp_roles: list[str] | None) -> str:
	roles = set(erp_roles or [])
	if roles & ADMIN_WMS_ROLES:
		return "admin"
	if roles & OPERATOR_WMS_ROLES:
		return "operator"
	if WMS_WAREHOUSE_USER_ROLE in roles:
		return "operator"
	return "operator"


def _fetch_roles_by_user(user_names: list[str]) -> dict[str, list[str]]:
	if not user_names:
		return {}

	rows = frappe.get_all(
		"Has Role",
		filters={"parenttype": "User", "parent": ["in", user_names]},
		fields=["parent", "role"],
	)
	out: dict[str, list[str]] = {}
	for row in rows:
		out.setdefault(row.parent, []).append(row.role)
	return out


def _fetch_employee_mobiles(user_names: list[str]) -> dict[str, str]:
	if not user_names:
		return {}

	rows = frappe.get_all(
		"Employee",
		filters={"user_id": ["in", user_names], "status": "Active"},
		fields=["user_id", "cell_number"],
	)
	return {row.user_id: row.cell_number for row in rows if row.get("user_id") and row.get("cell_number")}


def _compact_user_row(row: dict, *, roles: list[str], employee_mobile: str | None, fields: list[str]) -> dict:
	updated = row.get("custom_wms_modified") or row.get("modified")
	mobile = row.get("mobile_no") or employee_mobile

	payload = {
		"user_code": row.get("name"),
		"name": row.get("full_name") or row.get("name"),
		"role": _map_wms_role(roles),
		"active": bool(row.get("enabled")),
		"email": row.get("email"),
		"mobile_no": mobile,
		"updated_on": updated,
	}

	if not fields:
		return payload
	return {key: payload.get(key) for key in fields if key in payload}


def update_custom_wms_modified(doc, method=None):
	"""Touch WMS sync cursor when a warehouse user record changes."""
	try:
		if not doc or not getattr(doc, "meta", None):
			return
		if not doc.meta.has_field("custom_wms_modified"):
			return
		if doc.meta.name == "User":
			if not cint(getattr(doc, "custom_is_warehouse_user", 0)):
				return
			if WMS_WAREHOUSE_USER_ROLE not in {r.role for r in (doc.roles or [])}:
				return
		doc.custom_wms_modified = now()
	except Exception:
		pass


def touch_user_wms_modified_from_has_role(doc, method=None):
	"""Refresh parent user cursor when Warehouse User role assignment changes."""
	try:
		if not doc or doc.parenttype != "User" or doc.role != WMS_WAREHOUSE_USER_ROLE:
			return
		if not frappe.db.exists("User", doc.parent):
			return
		user = frappe.get_doc("User", doc.parent)
		if not cint(getattr(user, "custom_is_warehouse_user", 0)):
			return
		if user.meta.has_field("custom_wms_modified"):
			frappe.db.set_value("User", doc.parent, "custom_wms_modified", now(), update_modified=False)
	except Exception:
		pass


@frappe.whitelist(allow_guest=False)
def get_users_compact(
	filters=None,
	fields=None,
	limit=100,
	offset=0,
	custom_wms_modified_after=None,
):
	"""
	GET/POST /api/method/printechs_wms.api.user.get_users_compact

	Returns warehouse users for WMS sync. A user is included only when:
	  - User.enabled = 1
	  - custom_is_warehouse_user = 1
	  - Has Role: Warehouse User

	Response shape mirrors get_items_compact (users, limit, offset, has_more, max_custom_wms_modified).
	"""
	if not _user_has_wms_field("custom_is_warehouse_user"):
		frappe.throw(
			"User custom field custom_is_warehouse_user is not installed. Run bench migrate for printechs_wms."
		)

	filters = _ensure_dict(filters)
	fields = _ensure_list(fields) or DEFAULT_FIELDS
	limit = cint(limit) or 100
	offset = cint(offset) or 0

	role_holders = _warehouse_user_role_holders()
	if not role_holders:
		return {
			"users": [],
			"limit": limit,
			"offset": offset,
			"has_more": False,
			"max_custom_wms_modified": None,
		}

	db_fields = ["name", "full_name", "email", "mobile_no", "enabled", "modified"]
	if _user_has_wms_field("custom_wms_modified"):
		db_fields.append("custom_wms_modified")

	user_filters = {
		"enabled": 1,
		"custom_is_warehouse_user": 1,
		"name": ["in", list(role_holders)],
		**filters,
	}

	if custom_wms_modified_after and _user_has_wms_field("custom_wms_modified"):
		user_filters["custom_wms_modified"] = (">", custom_wms_modified_after)

	order_by = "custom_wms_modified asc" if _user_has_wms_field("custom_wms_modified") else "modified asc"

	rows = frappe.get_all(
		"User",
		filters=user_filters,
		fields=db_fields,
		order_by=order_by,
		limit_start=offset,
		limit_page_length=limit + 1,
	)

	has_more = len(rows) > limit
	if has_more:
		rows = rows[:limit]

	user_names = [row.name for row in rows if row.get("name")]
	roles_by_user = _fetch_roles_by_user(user_names)
	employee_mobiles = _fetch_employee_mobiles(user_names)

	users = []
	for row in rows:
		name = row.get("name")
		roles = roles_by_user.get(name, [])
		if WMS_WAREHOUSE_USER_ROLE not in roles:
			continue
		users.append(
			_compact_user_row(
				row,
				roles=roles,
				employee_mobile=employee_mobiles.get(name),
				fields=fields,
			)
		)

	cursor_field = "custom_wms_modified" if _user_has_wms_field("custom_wms_modified") else "modified"
	max_custom_wms_modified = rows[-1].get(cursor_field) if rows else None

	return {
		"users": users,
		"limit": limit,
		"offset": offset,
		"has_more": has_more,
		"max_custom_wms_modified": max_custom_wms_modified,
	}
