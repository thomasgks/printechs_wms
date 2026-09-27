import frappe

from printechs_wms.api.stock_balance_indexes import ensure_wms_stock_balance_indexes


def execute():
    ensure_wms_stock_balance_indexes(dry_run=0)
