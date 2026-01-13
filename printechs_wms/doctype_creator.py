import frappe
from frappe import _

def create_printtechs_wms_doctypes():
    """Create all custom DocTypes for Printechs WMS - SINGLE FUNCTION VERSION"""
    
    print("Starting creation of Printechs WMS DocTypes...")
    
    # Check if App exists, create if not
    if not frappe.db.exists("Module Def", "Printechs WMS"):
        print("Creating Printechs WMS Module...")
        app = frappe.get_doc({
            "doctype": "Module Def",
            "module_name": "Printechs WMS",
            "app_name": "Printechs WMS",
            "custom": 1
        })
        app.insert(ignore_permissions=True)
        frappe.db.commit()
        print("✓ Created Printechs WMS Module")
    
    # CREATE CHILD TABLES FIRST (simple ones without custom doctype links)
    print("\n=== Creating Child Tables ===")
    
    # 1. ASN Item Details
    if not frappe.db.exists("DocType", "ASN Item Details"):
        print("Creating ASN Item Details...")
        asn_items = frappe.get_doc({
            "doctype": "DocType",
            "module": "Printechs WMS",
            "custom": 1,
            "istable": 1,
            "name": "ASN Item Details",
            "fields": [
                {
                    "fieldname": "item_code",
                    "label": "Item Code",
                    "fieldtype": "Link",
                    "options": "Item",
                    "reqd": 1
                },
                {
                    "fieldname": "po_item_reference",
                    "label": "PO Item Reference",
                    "fieldtype": "Link",
                    "options": "Purchase Order Item"
                },
                {
                    "fieldname": "shipped_qty",
                    "label": "Shipped Qty",
                    "fieldtype": "Float",
                    "reqd": 1
                },
                {
                    "fieldname": "carton_id",
                    "label": "Carton ID",
                    "fieldtype": "Data"
                },
                {
                    "fieldname": "carton_assigned_status",
                    "label": "Carton Assigned Status",
                    "fieldtype": "Select",
                    "options": "\nAssigned\nMissing",
                    "reqd": 1,
                    "default": "Assigned"
                }
            ]
        })
        asn_items.insert(ignore_permissions=True)
        frappe.db.commit()
        print("✓ Created ASN Item Details Child Table")
    
    # 2. DP Store Allocation
    if not frappe.db.exists("DocType", "DP Store Allocation"):
        print("Creating DP Store Allocation...")
        dp_store = frappe.get_doc({
            "doctype": "DocType",
            "module": "Printechs WMS",
            "custom": 1,
            "istable": 1,
            "name": "DP Store Allocation",
            "fields": [
                {
                    "fieldname": "store",
                    "label": "Store",
                    "fieldtype": "Link",
                    "options": "Warehouse",
                    "reqd": 1
                },
                {
                    "fieldname": "allocated_qty",
                    "label": "Allocated Qty",
                    "fieldtype": "Float",
                    "reqd": 1
                }
            ]
        })
        dp_store.insert(ignore_permissions=True)
        frappe.db.commit()
        print("✓ Created DP Store Allocation Child Table")
    
    # 3. DP Allocation Details
    if not frappe.db.exists("DocType", "DP Allocation Details"):
        print("Creating DP Allocation Details...")
        dp_allocation = frappe.get_doc({
            "doctype": "DocType",
            "module": "Printechs WMS",
            "custom": 1,
            "istable": 1,
            "name": "DP Allocation Details",
            "fields": [
                {
                    "fieldname": "item_code",
                    "label": "Item Code",
                    "fieldtype": "Link",
                    "options": "Item",
                    "reqd": 1
                },
                {
                    "fieldname": "shipped_qty_asn",
                    "label": "Shipped Qty (ASN)",
                    "fieldtype": "Float",
                    "reqd": 1
                },
                {
                    "fieldname": "total_allocated_qty",
                    "label": "Total Allocated Qty",
                    "fieldtype": "Float",
                    "reqd": 1,
                    "read_only": 1
                },
                {
                    "fieldname": "allocations",
                    "label": "Allocations",
                    "fieldtype": "Table",
                    "options": "DP Store Allocation",
                    "reqd": 1
                }
            ]
        })
        dp_allocation.insert(ignore_permissions=True)
        frappe.db.commit()
        print("✓ Created DP Allocation Details Child Table")
    
    # 4. WMS Container Item
    if not frappe.db.exists("DocType", "WMS Container Item"):
        print("Creating WMS Container Item...")
        container_item = frappe.get_doc({
            "doctype": "DocType",
            "module": "Printechs WMS",
            "custom": 1,
            "istable": 1,
            "name": "WMS Container Item",
            "fields": [
                {
                    "fieldname": "item",
                    "label": "Item",
                    "fieldtype": "Link",
                    "options": "Item",
                    "reqd": 1
                },
                {
                    "fieldname": "quantity",
                    "label": "Quantity",
                    "fieldtype": "Float",
                    "reqd": 1
                },
                {
                    "fieldname": "uom",
                    "label": "UOM",
                    "fieldtype": "Link",
                    "options": "UOM",
                    "reqd": 1
                },
                {
                    "fieldname": "batch_no_serial_no",
                    "label": "Batch No/Serial No",
                    "fieldtype": "Data"
                }
            ]
        })
        container_item.insert(ignore_permissions=True)
        frappe.db.commit()
        print("✓ Created WMS Container Item Child Table")
    
    # 5. WMS Active Users
    if not frappe.db.exists("DocType", "WMS Active Users"):
        print("Creating WMS Active Users...")
        active_users = frappe.get_doc({
            "doctype": "DocType",
            "module": "Printechs WMS",
            "custom": 1,
            "istable": 1,
            "name": "WMS Active Users",
            "fields": [
                {
                    "fieldname": "user",
                    "label": "User",
                    "fieldtype": "Link",
                    "options": "User",
                    "reqd": 1
                },
                {
                    "fieldname": "assigned_items_count",
                    "label": "Assigned Items Count",
                    "fieldtype": "Int",
                    "default": 0
                },
                {
                    "fieldname": "completed_items_count",
                    "label": "Completed Items Count",
                    "fieldtype": "Int",
                    "default": 0
                },
                {
                    "fieldname": "status",
                    "label": "Status",
                    "fieldtype": "Select",
                    "options": "\nActive\nPaused\nSigned Out",
                    "default": "Active"
                }
            ]
        })
        active_users.insert(ignore_permissions=True)
        frappe.db.commit()
        print("✓ Created WMS Active Users Child Table")
    
    # CREATE PARENT DOCUMENTS (basic ones first)
    print("\n=== Creating Parent DocTypes ===")
    
    # 1. WMS Zone
    if not frappe.db.exists("DocType", "WMS Zone"):
        print("Creating WMS Zone...")
        zone = frappe.get_doc({
            "doctype": "DocType",
            "module": "Printechs WMS",
            "custom": 1,
            "name": "WMS Zone",
            "autoname": "field:zone_name",
            "title_field": "zone_name",
            "search_fields": "zone_name,warehouse",
            "track_seen": 1,
            "show_name_in_global_search": 1,
            "fields": [
                {
                    "fieldname": "zone_name",
                    "label": "Zone Name",
                    "fieldtype": "Data",
                    "reqd": 1,
                    "unique": 1
                },
                {
                    "fieldname": "warehouse",
                    "label": "Warehouse",
                    "fieldtype": "Link",
                    "options": "Warehouse",
                    "reqd": 1
                },
                {
                    "fieldname": "system_id",
                    "label": "System ID",
                    "fieldtype": "Data",
                    "read_only": 1
                },
                {
                    "fieldname": "zone_type",
                    "label": "Zone Type",
                    "fieldtype": "Select",
                    "options": "\nHierarchical\nFlat/Open",
                    "reqd": 1
                }
            ],
            "permissions": [
                {
                    "role": "System Manager",
                    "read": 1,
                    "write": 1,
                    "create": 1,
                    "delete": 1
                },
                {
                    "role": "Stock Manager",
                    "read": 1,
                    "write": 1,
                    "create": 1,
                    "delete": 1
                }
            ]
        })
        zone.insert(ignore_permissions=True)
        frappe.db.commit()
        print("✓ Created WMS Zone DocType")
    
    # 2. WMS Aisle
    if not frappe.db.exists("DocType", "WMS Aisle"):
        print("Creating WMS Aisle...")
        aisle = frappe.get_doc({
            "doctype": "DocType",
            "module": "Printechs WMS",
            "custom": 1,
            "name": "WMS Aisle",
            "autoname": "field:aisle_name",
            "title_field": "aisle_name",
            "search_fields": "aisle_name,parent_zone",
            "track_seen": 1,
            "show_name_in_global_search": 1,
            "fields": [
                {
                    "fieldname": "aisle_name",
                    "label": "Aisle Name",
                    "fieldtype": "Data",
                    "reqd": 1,
                    "unique": 1
                },
                {
                    "fieldname": "parent_zone",
                    "label": "Parent Zone",
                    "fieldtype": "Link",
                    "options": "WMS Zone",
                    "reqd": 1
                },
                {
                    "fieldname": "warehouse",
                    "label": "Warehouse",
                    "fieldtype": "Link",
                    "options": "Warehouse",
                    "reqd": 1
                },
                {
                    "fieldname": "system_id",
                    "label": "System ID",
                    "fieldtype": "Data",
                    "read_only": 1
                }
            ],
            "permissions": [
                {
                    "role": "System Manager",
                    "read": 1,
                    "write": 1,
                    "create": 1,
                    "delete": 1
                },
                {
                    "role": "Stock Manager",
                    "read": 1,
                    "write": 1,
                    "create": 1,
                    "delete": 1
                }
            ]
        })
        aisle.insert(ignore_permissions=True)
        frappe.db.commit()
        print("✓ Created WMS Aisle DocType")
    
    # 3. WMS Rack
    if not frappe.db.exists("DocType", "WMS Rack"):
        print("Creating WMS Rack...")
        rack = frappe.get_doc({
            "doctype": "DocType",
            "module": "Printechs WMS",
            "custom": 1,
            "name": "WMS Rack",
            "autoname": "field:rack_name",
            "title_field": "rack_name",
            "search_fields": "rack_name,parent_aisle",
            "track_seen": 1,
            "show_name_in_global_search": 1,
            "fields": [
                {
                    "fieldname": "rack_name",
                    "label": "Rack Name",
                    "fieldtype": "Data",
                    "reqd": 1,
                    "unique": 1
                },
                {
                    "fieldname": "parent_aisle",
                    "label": "Parent Aisle",
                    "fieldtype": "Link",
                    "options": "WMS Aisle",
                    "reqd": 1
                },
                {
                    "fieldname": "parent_zone",
                    "label": "Parent Zone",
                    "fieldtype": "Link",
                    "options": "WMS Zone",
                    "read_only": 1
                },
                {
                    "fieldname": "warehouse",
                    "label": "Warehouse",
                    "fieldtype": "Link",
                    "options": "Warehouse",
                    "read_only": 1
                },
                {
                    "fieldname": "system_id",
                    "label": "System ID",
                    "fieldtype": "Data",
                    "read_only": 1
                }
            ],
            "permissions": [
                {
                    "role": "System Manager",
                    "read": 1,
                    "write": 1,
                    "create": 1,
                    "delete": 1
                },
                {
                    "role": "Stock Manager",
                    "read": 1,
                    "write": 1,
                    "create": 1,
                    "delete": 1
                }
            ]
        })
        rack.insert(ignore_permissions=True)
        frappe.db.commit()
        print("✓ Created WMS Rack DocType")
    
    # 4. WMS Location
    if not frappe.db.exists("DocType", "WMS Location"):
        print("Creating WMS Location...")
        location = frappe.get_doc({
            "doctype": "DocType",
            "module": "Printechs WMS",
            "custom": 1,
            "name": "WMS Location",
            "autoname": "field:location_id",
            "title_field": "location_id",
            "search_fields": "location_id,warehouse,zone",
            "track_seen": 1,
            "show_name_in_global_search": 1,
            "fields": [
                {
                    "fieldname": "location_id",
                    "label": "Location ID",
                    "fieldtype": "Data",
                    "reqd": 1,
                    "unique": 1
                },
                {
                    "fieldname": "warehouse",
                    "label": "Warehouse",
                    "fieldtype": "Link",
                    "options": "Warehouse",
                    "reqd": 1
                },
                {
                    "fieldname": "zone",
                    "label": "Zone",
                    "fieldtype": "Link",
                    "options": "WMS Zone",
                    "reqd": 1
                },
                {
                    "fieldname": "aisle",
                    "label": "Aisle",
                    "fieldtype": "Data"
                },
                {
                    "fieldname": "parent_rack",
                    "label": "Parent Rack",
                    "fieldtype": "Link",
                    "options": "WMS Rack",
                    "reqd": 1
                },
                {
                    "fieldname": "level",
                    "label": "Level",
                    "fieldtype": "Data"
                },
                {
                    "fieldname": "bin_id",
                    "label": "Bin ID",
                    "fieldtype": "Data"
                },
                {
                    "fieldname": "location_type",
                    "label": "Location Type",
                    "fieldtype": "Select",
                    "options": "\nPicking\nBulk Storage\nStaging\nQC Hold"
                },
                {
                    "fieldname": "is_available",
                    "label": "Is Available",
                    "fieldtype": "Check",
                    "default": 1
                },
                {
                    "fieldname": "capacity",
                    "label": "Capacity (Volume/Weight)",
                    "fieldtype": "Float"
                }
            ],
            "permissions": [
                {
                    "role": "System Manager",
                    "read": 1,
                    "write": 1,
                    "create": 1,
                    "delete": 1
                },
                {
                    "role": "Stock Manager",
                    "read": 1,
                    "write": 1,
                    "create": 1,
                    "delete": 1
                },
                {
                    "role": "Stock User",
                    "read": 1,
                    "write": 1,
                    "create": 1,
                    "delete": 0
                }
            ]
        })
        location.insert(ignore_permissions=True)
        frappe.db.commit()
        print("✓ Created WMS Location DocType")
    
    # 5. WMS Container
    if not frappe.db.exists("DocType", "WMS Container"):
        print("Creating WMS Container...")
        container = frappe.get_doc({
            "doctype": "DocType",
            "module": "Printechs WMS",
            "custom": 1,
            "name": "WMS Container",
            "autoname": "LPN-.#####",
            "naming_series": "LPN-",
            "title_field": "container_id",
            "search_fields": "container_id,container_type,status",
            "track_seen": 1,
            "show_name_in_global_search": 1,
            "fields": [
                {
                    "fieldname": "container_id",
                    "label": "Container ID",
                    "fieldtype": "Data",
                    "reqd": 1,
                    "read_only": 1
                },
                {
                    "fieldname": "container_type",
                    "label": "Container Type",
                    "fieldtype": "Select",
                    "options": "\nCarton\nPallet\nTote",
                    "reqd": 1
                },
                {
                    "fieldname": "status",
                    "label": "Status",
                    "fieldtype": "Select",
                    "options": "\nEmpty\nLoaded\nStaged\nShipped",
                    "default": "Empty",
                    "reqd": 1
                },
                {
                    "fieldname": "current_bin",
                    "label": "Current Bin",
                    "fieldtype": "Link",
                    "options": "WMS Location",
                    "reqd": 1
                },
                {
                    "fieldname": "tare_weight",
                    "label": "Tare Weight",
                    "fieldtype": "Float"
                },
                {
                    "fieldname": "contents_section",
                    "label": "Contents",
                    "fieldtype": "Section Break"
                },
                {
                    "fieldname": "contents",
                    "label": "Contents",
                    "fieldtype": "Table",
                    "options": "WMS Container Item",
                    "reqd": 0
                }
            ],
            "permissions": [
                {
                    "role": "System Manager",
                    "read": 1,
                    "write": 1,
                    "create": 1,
                    "delete": 1
                },
                {
                    "role": "Stock Manager",
                    "read": 1,
                    "write": 1,
                    "create": 1,
                    "delete": 1
                },
                {
                    "role": "Stock User",
                    "read": 1,
                    "write": 1,
                    "create": 1,
                    "delete": 0
                }
            ]
        })
        container.insert(ignore_permissions=True)
        frappe.db.commit()
        print("✓ Created WMS Container DocType")
    
    # 6. Advance Shipping Notice
    if not frappe.db.exists("DocType", "Advance Shipping Notice"):
        print("Creating Advance Shipping Notice...")
        asn = frappe.get_doc({
            "doctype": "DocType",
            "module": "Printechs WMS",
            "custom": 1,
            "name": "Advance Shipping Notice",
            "autoname": "ASN-.#####",
            "naming_series": "ASN-",
            "title_field": "title",
            "search_fields": "title,supplier,status",
            "track_seen": 1,
            "is_submittable": 1,
            "show_name_in_global_search": 1,
            "quick_entry": 1,
            "track_changes": 1,
            "fields": [
                {
                    "fieldname": "title",
                    "label": "Title",
                    "fieldtype": "Data",
                    "reqd": 1,
                    "read_only": 1
                },
                {
                    "fieldname": "status",
                    "label": "Status",
                    "fieldtype": "Select",
                    "options": "\nDraft\nSubmitted\nApproved\nReceiving\nCompleted",
                    "default": "Draft",
                    "reqd": 1
                },
                {
                    "fieldname": "purchase_order",
                    "label": "Purchase Order",
                    "fieldtype": "Link",
                    "options": "Purchase Order",
                    "reqd": 1
                },
                {
                    "fieldname": "supplier",
                    "label": "Supplier",
                    "fieldtype": "Link",
                    "options": "Supplier",
                    "reqd": 1
                },
                {
                    "fieldname": "shipment_date",
                    "label": "Shipment Date",
                    "fieldtype": "Date",
                    "reqd": 1
                },
                {
                    "fieldname": "expected_arrival_date",
                    "label": "Expected Arrival Date",
                    "fieldtype": "Date",
                    "reqd": 1
                },
                {
                    "fieldname": "total_shipped_qty",
                    "label": "Total Shipped Qty",
                    "fieldtype": "Float",
                    "reqd": 1
                },
                {
                    "fieldname": "airway_bill_no",
                    "label": "Airway Bill No",
                    "fieldtype": "Data"
                },
                {
                    "fieldname": "shipment_type",
                    "label": "Shipment Type",
                    "fieldtype": "Select",
                    "options": "\nAir\nSea\nRoad"
                },
                {
                    "fieldname": "details_section",
                    "label": "Item Details",
                    "fieldtype": "Section Break"
                },
                {
                    "fieldname": "details",
                    "label": "Details",
                    "fieldtype": "Table",
                    "options": "ASN Item Details",
                    "reqd": 1
                }
            ],
            "permissions": [
                {
                    "role": "System Manager",
                    "read": 1,
                    "write": 1,
                    "create": 1,
                    "delete": 1,
                    "submit": 1,
                    "cancel": 1,
                    "amend": 1
                },
                {
                    "role": "Stock Manager",
                    "read": 1,
                    "write": 1,
                    "create": 1,
                    "delete": 1,
                    "submit": 1,
                    "cancel": 1,
                    "amend": 1
                }
            ]
        })
        asn.insert(ignore_permissions=True)
        frappe.db.commit()
        print("✓ Created Advance Shipping Notice DocType")
    
    # 7. Distribution Plan
    if not frappe.db.exists("DocType", "Distribution Plan"):
        print("Creating Distribution Plan...")
        dp = frappe.get_doc({
            "doctype": "DocType",
            "module": "Printechs WMS",
            "custom": 1,
            "name": "Distribution Plan",
            "autoname": "DP-.#####",
            "naming_series": "DP-",
            "title_field": "title",
            "search_fields": "title,status,brand_manager",
            "track_seen": 1,
            "is_submittable": 1,
            "show_name_in_global_search": 1,
            "quick_entry": 1,
            "track_changes": 1,
            "fields": [
                {
                    "fieldname": "title",
                    "label": "Title",
                    "fieldtype": "Data",
                    "reqd": 1,
                    "read_only": 1
                },
                {
                    "fieldname": "status",
                    "label": "Status",
                    "fieldtype": "Select",
                    "options": "\nDraft\nSubmitted\nApproved\nExecuted",
                    "default": "Draft",
                    "reqd": 1
                },
                {
                    "fieldname": "advance_shipping_notice",
                    "label": "Advance Shipping Notice",
                    "fieldtype": "Link",
                    "options": "Advance Shipping Notice",
                    "reqd": 1
                },
                {
                    "fieldname": "brand_manager",
                    "label": "Brand Manager",
                    "fieldtype": "Link",
                    "options": "User",
                    "reqd": 1
                },
                {
                    "fieldname": "allocation_section",
                    "label": "Allocation Details",
                    "fieldtype": "Section Break"
                },
                {
                    "fieldname": "allocation_details",
                    "label": "Allocation Details",
                    "fieldtype": "Table",
                    "options": "DP Allocation Details",
                    "reqd": 1
                }
            ],
            "permissions": [
                {
                    "role": "System Manager",
                    "read": 1,
                    "write": 1,
                    "create": 1,
                    "delete": 1,
                    "submit": 1,
                    "cancel": 1,
                    "amend": 1
                },
                {
                    "role": "Brand Manager",
                    "read": 1,
                    "write": 1,
                    "create": 1,
                    "delete": 1,
                    "submit": 1,
                    "cancel": 1,
                    "amend": 1
                }
            ]
        })
        dp.insert(ignore_permissions=True)
        frappe.db.commit()
        print("✓ Created Distribution Plan DocType")
    
    # NOW CREATE THE CHILD TABLE THAT REFERENCES CUSTOM DOCTYPES
    print("\n=== Creating WMS Transaction Item Detail ===")
    
    # 8. WMS Transaction Item Detail (needs WMS Container and WMS Location)
    if not frappe.db.exists("DocType", "WMS Transaction Item Detail"):
        print("Creating WMS Transaction Item Detail (basic fields first)...")
        item_detail = frappe.get_doc({
            "doctype": "DocType",
            "module": "Printechs WMS",
            "custom": 1,
            "istable": 1,
            "name": "WMS Transaction Item Detail",
            "fields": [
                {
                    "fieldname": "item",
                    "label": "Item",
                    "fieldtype": "Link",
                    "options": "Item",
                    "reqd": 1
                },
                {
                    "fieldname": "quantity",
                    "label": "Quantity",
                    "fieldtype": "Float",
                    "reqd": 1
                },
                {
                    "fieldname": "uom",
                    "label": "UOM",
                    "fieldtype": "Link",
                    "options": "UOM",
                    "reqd": 1
                },
                {
                    "fieldname": "actual_qty_counted",
                    "label": "Actual Qty Counted",
                    "fieldtype": "Float"
                },
                {
                    "fieldname": "discrepancy",
                    "label": "Discrepancy",
                    "fieldtype": "Float",
                    "read_only": 1
                },
                {
                    "fieldname": "assignment_status",
                    "label": "Assignment Status",
                    "fieldtype": "Select",
                    "options": "\nPending\nAssigned\nIn Progress\nDone\nSkipped",
                    "default": "Pending"
                },
                {
                    "fieldname": "assigned_operator",
                    "label": "Assigned Operator",
                    "fieldtype": "Link",
                    "options": "User"
                },
                {
                    "fieldname": "actual_completion_time",
                    "label": "Actual Completion Time",
                    "fieldtype": "Datetime"
                }
            ]
        })
        item_detail.insert(ignore_permissions=True)
        frappe.db.commit()
        print("✓ Created WMS Transaction Item Detail Child Table (basic fields)")
        
        # Now add the link fields using custom field creation
        print("Adding custom doctype links to WMS Transaction Item Detail...")
        
        # Create custom fields for the links
        custom_fields = {
            "WMS Transaction Item Detail": [
                {
                    "fieldname": "container_id",
                    "label": "Container ID",
                    "fieldtype": "Link",
                    "options": "WMS Container",
                    "insert_after": "uom"
                },
                {
                    "fieldname": "source_bin",
                    "label": "Source Bin",
                    "fieldtype": "Link",
                    "options": "WMS Location",
                    "insert_after": "container_id"
                },
                {
                    "fieldname": "target_bin",
                    "label": "Target Bin",
                    "fieldtype": "Link",
                    "options": "WMS Location",
                    "insert_after": "source_bin"
                }
            ]
        }
        
        # Import and use create_custom_fields
        from frappe.custom.doctype.custom_field.custom_field import create_custom_fields
        create_custom_fields(custom_fields)
        frappe.db.commit()
        print("✓ Added custom doctype links to WMS Transaction Item Detail")
    
    # 9. WMS Transaction
    if not frappe.db.exists("DocType", "WMS Transaction"):
        print("Creating WMS Transaction...")
        transaction = frappe.get_doc({
            "doctype": "DocType",
            "module": "Printechs WMS",
            "custom": 1,
            "name": "WMS Transaction",
            "autoname": "WMS-TRANS-.#####",
            "naming_series": "WMS-TRANS-",
            "title_field": "title",
            "search_fields": "title,operation_type,status",
            "track_seen": 1,
            "is_submittable": 1,
            "show_name_in_global_search": 1,
            "track_changes": 1,
            "fields": [
                {
                    "fieldname": "title",
                    "label": "Title",
                    "fieldtype": "Data",
                    "reqd": 1,
                    "read_only": 1
                },
                {
                    "fieldname": "status",
                    "label": "Status",
                    "fieldtype": "Select",
                    "options": "\nDraft\nSubmitted\nCompleted",
                    "default": "Draft",
                    "reqd": 1
                },
                {
                    "fieldname": "operation_type",
                    "label": "Operation Type",
                    "fieldtype": "Select",
                    "options": "\nReceiving\nSorting\nPutaway\nPicking\nBin Move\nCycle Count\nStock Adjustment",
                    "reqd": 1
                },
                {
                    "fieldname": "transaction_date",
                    "label": "Transaction Date",
                    "fieldtype": "Datetime",
                    "reqd": 1,
                    "default": "now"
                },
                {
                    "fieldname": "assigned_to",
                    "label": "Assigned To",
                    "fieldtype": "Link",
                    "options": "User",
                    "reqd": 1
                },
                {
                    "fieldname": "source_warehouse",
                    "label": "Source Warehouse",
                    "fieldtype": "Link",
                    "options": "Warehouse",
                    "reqd": 1
                },
                {
                    "fieldname": "target_warehouse",
                    "label": "Target Warehouse",
                    "fieldtype": "Link",
                    "options": "Warehouse",
                    "reqd": 1
                },
                {
                    "fieldname": "reference_type",
                    "label": "Reference Type",
                    "fieldtype": "Select",
                    "options": "\nAdvance Shipping Notice\nPurchase Order\nStock Entry\nMaterial Request"
                },
                {
                "fieldname": "reference_doc_name",
                "label": "Reference Document",
                "fieldtype": "Dynamic Link",
                "options": "reference_type"
                },
                {
                    "fieldname": "transaction_status",
                    "label": "Transaction Status",
                    "fieldtype": "Select",
                    "options": "\nDraft\nIn Planning\nIn Progress\nPartial\nCompleted",
                    "default": "Draft"
                },
                {
                    "fieldname": "primary_assignee",
                    "label": "Primary Assignee",
                    "fieldtype": "Link",
                    "options": "User"
                },
                {
                    "fieldname": "completion_progress",
                    "label": "Completion Progress",
                    "fieldtype": "Percent",
                    "read_only": 1
                },
                {
                    "fieldname": "is_locked",
                    "label": "Is Locked",
                    "fieldtype": "Check",
                    "default": 0
                },
                {
                    "fieldname": "details_section",
                    "label": "Transaction Details",
                    "fieldtype": "Section Break"
                },
                {
                    "fieldname": "details",
                    "label": "Details",
                    "fieldtype": "Table",
                    "options": "WMS Transaction Item Detail",
                    "reqd": 1
                },
                {
                    "fieldname": "concurrent_users_section",
                    "label": "Active Users",
                    "fieldtype": "Section Break"
                },
                {
                    "fieldname": "concurrent_users",
                    "label": "Concurrent Users",
                    "fieldtype": "Table",
                    "options": "WMS Active Users"
                }
            ],
            "permissions": [
                {
                    "role": "System Manager",
                    "read": 1,
                    "write": 1,
                    "create": 1,
                    "delete": 1,
                    "submit": 1,
                    "cancel": 1,
                    "amend": 1
                },
                {
                    "role": "Stock Manager",
                    "read": 1,
                    "write": 1,
                    "create": 1,
                    "delete": 1,
                    "submit": 1,
                    "cancel": 1,
                    "amend": 1
                },
                {
                    "role": "Stock User",
                    "read": 1,
                    "write": 1,
                    "create": 1,
                    "delete": 0,
                    "submit": 1,
                    "cancel": 0,
                    "amend": 0
                }
            ]
        })
        transaction.insert(ignore_permissions=True)
        frappe.db.commit()
        print("✓ Created WMS Transaction DocType")
    
    print("\n" + "="*50)
    print("✓ All Printechs WMS DocTypes created successfully!")
    print("="*50)

def test():
    print("TEST OK")

# Execute the creation
if __name__ == "__main__":
    create_printtechs_wms_doctypes()