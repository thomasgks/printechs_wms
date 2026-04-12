# Missing / untracked API files

## Why files appear "missing"

1. **The `printechs_wms/api/` folder has never been committed to git.**  
   `git status` shows `?? printechs_wms/api/` (untracked). So:
   - Any `.py` files you add in `api/` exist only on this machine until you `git add` and `git commit` them.
   - If the repo was re-cloned, or you switched branch/computer, only what was committed (e.g. initial app structure) is present — so API modules can look "missing".

2. **References to modules that don’t exist**  
   - `wms_transfer_order.js` calls `printechs_wms.api.transfer_order_import.import_transfer_order_from_excel`  
     → **transfer_order_import.py** was missing; a stub has been added so the client doesn’t break.

## What currently exists under `printechs_wms/api/`

- `__init__.py`
- `offline_sync.py`
- `desktop_stock_entry.py`
- `item.py`
- `items.py`
- `location.py`
- `material_request.py`
- `transfer_in_sync.py`
- `transfer_order_import.py` (stub)
- `wms_sync.py` (recreated: get_asns_for_wms, get_tos_for_wms)
- `pull_sync.py` (recreated: get_asn_detail)
- `asn_receiving.py` (recreated: update_asn_received_qty)
- `asn_import.py` (stub)
- `asn_import_ui.py` (recreated: get_warehouse_code)
- `transfer_order_sync.py` (re-export of get_tos_for_wms)
- `offline_sync_desktop_mapping_snippet.py`
- `OFFLINE_SYNC_DESKTOP_MAPPING.md`

## What you should do

1. **Commit the API folder so nothing is lost again**  
   ```bash
   cd /path/to/apps/printechs_wms
   git add printechs_wms/api/
   git commit -m "Add API modules (offline_sync, item, location, etc.)"
   git push
   ```

2. **Restore other missing modules**  
   - From backup, or  
   - From another clone/branch that has them, or  
   - Recreate them and then commit.

3. **Stop relying on untracked code**  
   Always add and commit new or changed `.py` files so they’re in git and available on every clone/branch.
