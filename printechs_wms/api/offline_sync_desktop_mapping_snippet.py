# Copy these into your offline_sync.py (replace the relevant parts).

# --------------- 1) In upsert_wms_stock_ledger: replace the single line that sets doc.location ---------------

# REMOVE this line:
#   doc.location = row.get("target_bin") or row.get("bin_location")

# ADD this block instead:
to_bin = (row.get("target_bin") or row.get("to_bin") or "").strip()
from_bin_val = (row.get("bin_location") or row.get("from_bin") or "").strip()
doc.location = to_bin or from_bin_val  # required Bin Location (prefer destination for Putaway)
if not doc.location:
    frappe.throw(
        "stock_transactions[]: provide at least one of target_bin, to_bin, bin_location, from_bin"
    )
if hasattr(doc, "from_bin") and from_bin_val:
    doc.from_bin = from_bin_val
if hasattr(doc, "to_bin") and to_bin:
    doc.to_bin = to_bin

# --------------- 2) In upsert_wms_stock_balance: extend the location line ---------------

# REPLACE:
#   location = (row.get("bin_location") or row.get("location") or row.get("target_bin") or "").strip()
# WITH:
location = (
    (row.get("bin_location") or row.get("location") or row.get("target_bin")
    or row.get("to_bin") or row.get("from_bin") or ""
).strip()
