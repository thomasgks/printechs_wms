frappe.query_reports["WMS Cycle Count Scan Report"] = {
	filters: [
		{
			fieldname: "company",
			label: __("Company"),
			fieldtype: "Link",
			options: "Company",
			default: frappe.defaults.get_user_default("Company"),
		},
		{
			fieldname: "warehouse",
			label: __("Warehouse"),
			fieldtype: "Link",
			options: "Warehouse",
		},
		{
			fieldname: "from_date",
			label: __("Count From Date"),
			fieldtype: "Date",
			default: frappe.datetime.add_months(frappe.datetime.get_today(), -1),
		},
		{
			fieldname: "to_date",
			label: __("Count To Date"),
			fieldtype: "Date",
			default: frappe.datetime.get_today(),
		},
		{
			fieldname: "batch",
			label: __("Batch"),
			fieldtype: "Link",
			options: "WMS Cycle Count Batch",
		},
		{
			fieldname: "task",
			label: __("Task"),
			fieldtype: "Link",
			options: "WMS Cycle Count Task",
		},
		{
			fieldname: "bin_location",
			label: __("Location"),
			fieldtype: "Link",
			options: "WMS Bin Location",
		},
		{
			fieldname: "carton_id",
			label: __("Carton ID"),
			fieldtype: "Data",
		},
		{
			fieldname: "item_code",
			label: __("Item"),
			fieldtype: "Link",
			options: "Item",
		},
		{
			fieldname: "group_by",
			label: __("Group By"),
			fieldtype: "Select",
			options: "Detail\nLocation\nCarton\nItem\nTask",
			default: "Detail",
		},
		{
			fieldname: "only_included_in_post",
			label: __("Only Tasks Included in Post"),
			fieldtype: "Check",
			default: 0,
		},
		{
			fieldname: "limit",
			label: __("Limit"),
			fieldtype: "Int",
			default: 5000,
		},
	],
	formatter(value, row, column, data, default_formatter) {
		value = default_formatter(value, row, column, data);
		if (data && data.bold) {
			value = `<b>${value != null ? value : ""}</b>`;
		}
		return value;
	},
};
