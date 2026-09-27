# Cursor AI Script — Desktop WMS Correction Export

Copy everything inside the **PROMPT FOR CURSOR** block below into Cursor on the **desktop WMS project** (the app that owns `tabStockTransaction`).

Goal: produce one Excel file that ERP can use to **verify and correct auto-pick mismatches** on submitted Material Transfer Stock Entries.

---

## PROMPT FOR CURSOR (copy from here)

```
You are working on the Printechs WMS Desktop application (SQL Server / local DB with tabStockTransaction).

## Background
ERP submitted Material Transfer Stock Entries using AUTO-PICK (warehouse-level allocation).
Desktop recorded the CORRECT physical picks in tabStockTransaction (location + carton + qty).
ERP WMS Stock Ledger deducted from WRONG cartons in many cases.

We need ONE Excel export so ERP can align WMS ledger to desktop truth.

## End result required (what ERP will do with the file)
For each submitted Stock Entry linked to a Material Request:
- Reverse ERP WMS deductions on wrong cartons (if any)
- Apply WMS deductions on the desktop-confirmed location + carton + qty
- After correction: ERP WMS carton balance must match desktop picks per MR/STE

## What to export — one row per physical pick line
Include ONLY picking lines that belong to Material Requests that already have a SUBMITTED ERP Stock Entry
(remarks like "Push from WMS Desktop", stock_entry_type = Material Transfer).

Do NOT export item-only rows without bin_location and carton_id.

### Required Excel columns (exact header names, row 1)
| Column | Type | Required | Description |
|--------|------|----------|-------------|
| posting_date | date | Yes | Date transfer was posted / pushed to ERP |
| material_request | text | Yes | MR name, e.g. MR-2026-03724 |
| stock_entry | text | Yes if known | ERP STE name, e.g. MAT-STE-2026-20120 (join from ERP sync table if available) |
| item_code | text | Yes | Item code |
| bin_location | text | Yes | Source bin/location picked from |
| carton_id | text | Yes | Source carton picked from |
| desktop_qty | number | Yes | Positive qty picked (absolute value; use sum if multiple txn rows same MR+item+location+carton) |
| warehouse | text | Optional | e.g. WH-MAIN |
| desktop_txn_count | number | Optional | How many tabStockTransaction rows were aggregated |
| user_verified | text | Optional | Y / N — warehouse user confirmed this line |
| user_notes | text | Optional | Free text |

### Aggregation rule
Group tabStockTransaction picking rows by:
  material_request + item_code + bin_location + carton_id
Sum qty_change as desktop_qty (use ABS if stored negative).

Filter:
  transaction_type = 'Picking' (or equivalent)
  reference_doc_type = 'Material Request'
  qty_change < 0 (outbound pick)
  bin_location IS NOT NULL AND bin_location <> ''
  carton_id IS NOT NULL AND carton_id <> ''

### Scope
Export all MRs in the attached mismatch list OR all MRs that have a submitted ERP STE from desktop push.
Priority MR list is in ERP file: auto_pick_verification_mr_summary.tsv (130 MRs).

### Reference SQL (adapt table/column names to desktop DB)
```sql
SELECT
    CAST(t.posting_date AS date) AS posting_date,
    t.reference_doc AS material_request,
    t.item_code,
    t.bin_location,
    t.carton_id,
    SUM(ABS(t.qty_change)) AS desktop_qty,
    t.warehouse,
    COUNT(*) AS desktop_txn_count
FROM tabStockTransaction t
WHERE t.transaction_type = 'Picking'
  AND t.reference_doc_type = 'Material Request'
  AND t.qty_change < 0
  AND ISNULL(t.bin_location, '') <> ''
  AND ISNULL(t.carton_id, '') <> ''
  AND t.reference_doc IN ( /* list of 130 MR names from ERP summary file */ )
GROUP BY
    CAST(t.posting_date AS date),
    t.reference_doc,
    t.item_code,
    t.bin_location,
    t.carton_id,
    t.warehouse
ORDER BY posting_date, material_request, item_code, bin_location, carton_id;
```

If ERP stock_entry name is stored locally after sync, LEFT JOIN and add stock_entry column.

### Validation before saving Excel
1. Every row must have material_request, item_code, bin_location, carton_id, desktop_qty > 0
2. No duplicate keys: MR + item + bin_location + carton_id should appear once per file
3. Per MR + item_code: SUM(desktop_qty) should match total transferred qty for that item on the STE
4. Save as: WMS_Desktop_Confirmed_Picks.xlsx (sheet name: ConfirmedPicks)

### Example rows (illustrative)
posting_date | material_request | stock_entry | item_code | bin_location | carton_id | desktop_qty
2026-08-27 | MR-2026-03724 | MAT-STE-2026-20120 | 314636 | B006-OWSHEL-014B | CNWH48931 | 2
2026-08-27 | MR-2026-03724 | MAT-STE-2026-20120 | 314638 | B006-OWSHEL-015C | CNWH52128 | 2

### What NOT to include
- Rows without location/carton (these caused auto-pick in ERP)
- Draft / cancelled transfers not submitted in ERP
- IN transactions (positive qty_change)
- ERP auto-pick cartons unless desktop also picked that same carton (ERP-only wrong lines are handled server-side)

Deliverable: WMS_Desktop_Confirmed_Picks.xlsx ready to send back to ERP team for WMS ledger correction.
```

---

## For the ERP / warehouse user (after desktop export)

1. Warehouse users review desktop export — column `user_verified = Y` on each line (or send as-is if desktop is trusted).
2. Send file to ERP team: place in `printechs_wms/doc/WMS_Desktop_Confirmed_Picks.xlsx`
3. ERP will:
   - Compare file vs `auto_pick_verification_for_users.tsv`
   - Flag any remaining mismatches
   - Run WMS ledger correction (reverse wrong auto-pick + apply desktop cartons)
   - Re-verify until MR shows PASS

## Optional: fill ERP verification sheet instead

If you prefer not to build a new desktop export, you can use the existing ERP file:

**File:** `sites/moosa.live/private/auto_pick_verification_for_users.tsv`

Fill columns for rows where desktop is confirmed:
- `verified_correct_location`
- `verified_correct_carton`
- `verified_correct_qty`

Leave blank if desktop_qty / bin_location / carton_id are already correct.

Save as Excel and return — same correction process applies.

## Related ERP audit files (read-only)

| File | Purpose |
|------|---------|
| `auto_pick_verification_for_users.tsv` | 3,523 mismatch lines (desktop vs ERP side by side) |
| `auto_pick_verification_mr_summary.tsv` | 130 MRs summary — use as scope list for desktop export |
| `auto_pick_erp_transactions_for_users.tsv` | 3,152 ERP auto-pick lines (reference only) |
| `post_cycle_transfer_verify.tsv` | 33 MRs after Sep-13 cycle count |

All under: `/home/erpnext/frappe-bench/sites/moosa.live/private/`

## Important clarification

`DESKTOP_PICKED_NOT_IN_ERP` does **not** mean desktop failed to sync.

It means: desktop pick exists in tabStockTransaction, but ERP **WMS Stock Ledger** did not deduct that carton on the submitted Stock Entry (ERP auto-picked other cartons instead).

Desktop export = source of truth for correction.
