frappe.query_reports["WMS Stock Movement Report"] = {
  filters: [
    {
      fieldname: "company",
      label: __("Company"),
      fieldtype: "Link",
      options: "Company",
      reqd: 1,
      default: frappe.defaults.get_user_default("Company"),
    },
    {
      fieldname: "warehouse",
      label: __("Warehouse"),
      fieldtype: "Link",
      options: "Warehouse",
    },
    {
      fieldname: "item_code",
      label: __("Item"),
      fieldtype: "Link",
      options: "Item",
    },
    {
      fieldname: "location",
      label: __("Location"),
      fieldtype: "Link",
      options: "WMS Bin Location",
    },
    {
      fieldname: "carton",
      label: __("Carton"),
      fieldtype: "Data",
    },
    {
      fieldname: "event_type",
      label: __("Event Type"),
      fieldtype: "Select",
      options: "\nReceive\nPutaway\nTransfer\nPick\nCycleCount\nAdjust",
    },
    {
      fieldname: "from_date",
      label: __("From Datetime"),
      fieldtype: "Datetime",
    },
    {
      fieldname: "to_date",
      label: __("To Datetime"),
      fieldtype: "Datetime",
    },
    {
      fieldname: "limit",
      label: __("Limit"),
      fieldtype: "Int",
      default: 2000,
    },
  ],
};
