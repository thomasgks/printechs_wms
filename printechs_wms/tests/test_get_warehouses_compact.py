# Copyright (c) 2026, Printechs and contributors
# License: MIT. See license.txt

import frappe
from frappe.tests.utils import FrappeTestCase

from printechs_wms.api.warehouse import (
	get_warehouses_compact,
	is_active_warehouse,
	resolve_warehouse_docname,
	setup_warehouse_wms_fields,
)


class TestGetWarehousesCompact(FrappeTestCase):
	@classmethod
	def setUpClass(cls):
		super().setUpClass()
		setup_warehouse_wms_fields()

	def test_excludes_disabled_and_group_warehouses(self):
		leaf = frappe.db.get_value(
			"Warehouse",
			{"disabled": 0, "is_group": 0},
			["name", "warehouse_name", "code"],
			as_dict=True,
		)
		self.assertTrue(leaf)

		group = frappe.db.get_value("Warehouse", {"is_group": 1}, "name")
		self.assertTrue(group)

		result = get_warehouses_compact(limit=500)
		names = {row["name"] for row in result["warehouses"]}

		self.assertIn(leaf.name, names)
		self.assertNotIn(group, names)
		for row in result["warehouses"]:
			self.assertTrue(row["enabled"])

	def test_resolve_by_code_and_display_name(self):
		row = frappe.db.get_value(
			"Warehouse",
			{"disabled": 0, "is_group": 0, "code": ["!=", ""]},
			["name", "warehouse_name", "code"],
			as_dict=True,
		)
		self.assertTrue(row)

		by_code = resolve_warehouse_docname(code=row.code)
		self.assertEqual(by_code, row.name)

		by_label = resolve_warehouse_docname(warehouse_name=row.warehouse_name)
		self.assertEqual(by_label, row.name)

		self.assertTrue(is_active_warehouse(row.name))
