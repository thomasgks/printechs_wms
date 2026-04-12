frappe.query_reports["WMS Stock Balance Report"] = {
  filters: [
    {
      fieldname: "company",
      label: "Company",
      fieldtype: "Link",
      options: "Company",
      reqd: 1,
      default: frappe.defaults.get_user_default("Company"),
    },
    {
      fieldname: "warehouse",
      label: "Warehouse",
      fieldtype: "Link",
      options: "Warehouse",
      default: "Mohammed Abdullah Almousa Trading Company",
    },
    {
      fieldname: "item_code",
      label: "Item",
      fieldtype: "Link",
      options: "Item",
    },
    {
      fieldname: "location",
      label: "Location",
      fieldtype: "Data",
    },
    {
      fieldname: "carton",
      label: "Carton",
      fieldtype: "Data",
    },
    {
      fieldname: "only_positive_qty",
      label: "Only Positive Qty",
      fieldtype: "Check",
      default: 1,
    },
    {
      fieldname: "show_zero_qty",
      label: "Show Zero Qty",
      fieldtype: "Check",
      default: 0,
    },
    {
      fieldname: "limit",
      label: "Limit",
      fieldtype: "Int",
      default: 500,
    },
  ],
};
