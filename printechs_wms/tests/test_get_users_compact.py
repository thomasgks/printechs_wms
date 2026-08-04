# Copyright (c) 2026, Printechs and contributors
# License: MIT. See license.txt

import frappe
from frappe.tests.utils import FrappeTestCase

from printechs_wms.api.user import WMS_WAREHOUSE_USER_ROLE, get_users_compact


class TestGetUsersCompact(FrappeTestCase):
	@classmethod
	def setUpClass(cls):
		super().setUpClass()
		from printechs_wms.setup_warehouse_user import setup_warehouse_user_master

		setup_warehouse_user_master()

	def _make_warehouse_user(self, username: str, *, full_name: str, admin: bool = False):
		if frappe.db.exists("User", username):
			user = frappe.get_doc("User", username)
		else:
			user = frappe.get_doc(
				{
					"doctype": "User",
					"email": username,
					"first_name": full_name,
					"send_welcome_email": 0,
				}
			)
			user.insert(ignore_permissions=True)

		user.enabled = 1
		user.custom_is_warehouse_user = 1
		existing_roles = {row.role for row in (user.roles or [])}
		if WMS_WAREHOUSE_USER_ROLE not in existing_roles:
			user.append("roles", {"role": WMS_WAREHOUSE_USER_ROLE})
		if admin and "Stock Manager" not in existing_roles:
			user.append("roles", {"role": "Stock Manager"})
		user.save(ignore_permissions=True)
		return user

	def test_returns_only_flagged_warehouse_role_users(self):
		wms_user = self._make_warehouse_user("wms.operator@test.com", full_name="WMS Operator")
		self._make_warehouse_user("wms.admin@test.com", full_name="WMS Admin", admin=True)

		plain = frappe.get_doc(
			{
				"doctype": "User",
				"email": "plain.user@test.com",
				"first_name": "Plain",
				"send_welcome_email": 0,
				"enabled": 1,
			}
		)
		if frappe.db.exists("User", plain.name):
			plain = frappe.get_doc("User", plain.name)
		else:
			plain.insert(ignore_permissions=True)

		result = get_users_compact(limit=500)
		codes = {row["user_code"] for row in result["users"]}

		self.assertIn(wms_user.name, codes)
		self.assertNotIn(plain.name, codes)
		self.assertIn("limit", result)
		self.assertIn("has_more", result)

	def test_role_mapping(self):
		self._make_warehouse_user("wms.admin@test.com", full_name="WMS Admin", admin=True)
		result = get_users_compact(filters={"name": "wms.admin@test.com"})
		self.assertEqual(result["users"][0]["role"], "admin")

		self._make_warehouse_user("wms.operator@test.com", full_name="WMS Operator")
		result = get_users_compact(filters={"name": "wms.operator@test.com"})
		self.assertEqual(result["users"][0]["role"], "operator")
