from __future__ import annotations

import unittest

import frappe

from printechs_wms.api.stock_entry_material_request import (
    resolve_material_request,
    resolve_mr_item_name,
)


class TestStockEntryMaterialRequest(unittest.TestCase):
    def test_resolve_material_request_from_payload(self):
        self.assertEqual(
            resolve_material_request("MR-TEST-1", []),
            "MR-TEST-1",
        )

    def test_resolve_material_request_from_items(self):
        self.assertEqual(
            resolve_material_request(
                None,
                [{"item_code": "A", "material_request": "MR-TEST-2"}],
            ),
            "MR-TEST-2",
        )

    def test_resolve_mr_item_name_missing(self):
        self.assertIsNone(resolve_mr_item_name("", "ITEM-1"))
        self.assertIsNone(resolve_mr_item_name("MR-DOES-NOT-EXIST", "ITEM-1"))


def run_tests():
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(TestStockEntryMaterialRequest)
    runner = unittest.TextTestRunner(verbosity=2)
    return runner.run(suite)


if __name__ == "__main__":
    run_tests()
