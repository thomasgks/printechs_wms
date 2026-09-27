// Copyright (c) 2026, printechs and contributors
// For license information, please see license.txt

function wms_asn_get_receiving_warehouse(frm) {
	return (
		(frm.doc.default_receiving_warehouse_name || "").trim() ||
		(frm.doc.default_receiving_warehouse_code || "").trim()
	);
}

function wms_asn_prompt_warehouse(frm, callback) {
	const dialog = new frappe.ui.Dialog({
		title: __("Receiving Warehouse"),
		fields: [
			{
				fieldname: "warehouse",
				fieldtype: "Link",
				label: __("Warehouse"),
				options: "Warehouse",
				reqd: 1,
				default: wms_asn_get_receiving_warehouse(frm) || undefined,
			},
		],
		primary_action_label: __("Continue"),
		primary_action(values) {
			dialog.hide();
			callback(values.warehouse);
		},
	});
	dialog.show();
}

function wms_asn_create_purchase_receipt(frm, warehouse) {
	if (!warehouse) {
		frappe.msgprint(__("Receiving warehouse is required."));
		return;
	}

	frappe.call({
		method: "printechs_wms.api.asn_to_purchase_receipt.receive_asn_and_create_purchase_receipt",
		type: "POST",
		freeze: true,
		freeze_message: __("Creating Purchase Receipt..."),
		args: {
			asn_no: frm.doc.name,
			warehouse,
			make_pr: 1,
			group_items: 1,
		},
		callback(r) {
			const data = r.message || {};
			if (!data.ok) {
				frappe.msgprint({
					title: __("Purchase Receipt"),
					message: data.reason || __("Could not create Purchase Receipt."),
					indicator: "red",
				});
				return;
			}

			if (data.already_exists) {
				frappe.show_alert({
					message: __("Purchase Receipt already linked: {0}", [data.purchase_receipt]),
					indicator: "blue",
				});
			} else {
				frappe.show_alert({
					message: __("Draft Purchase Receipt {0} created", [data.purchase_receipt]),
					indicator: "green",
				});
			}

			frm.reload_doc().then(() => {
				if (data.purchase_receipt) {
					frappe.set_route("Form", "Purchase Receipt", data.purchase_receipt);
				}
			});
		},
	});
}

frappe.ui.form.on("WMS ASN", {
	refresh(frm) {
		if (frm.is_new()) {
			return;
		}

		if (frm.doc.purchase_receipt) {
			frm.add_custom_button(__("Open Purchase Receipt"), () => {
				frappe.set_route("Form", "Purchase Receipt", frm.doc.purchase_receipt);
			}, __("Actions"));
			return;
		}

		if (frm.doc.docstatus === 2) {
			return;
		}

		const received_qty = flt(frm.doc.total_received_qty);
		if (received_qty <= 0) {
			return;
		}

		frm.add_custom_button(__("Create Purchase Receipt (Draft)"), () => {
			frappe.confirm(
				__(
					"Create a draft Purchase Receipt from received quantities on this ASN?"
				),
				() => {
					const warehouse = wms_asn_get_receiving_warehouse(frm);
					if (warehouse) {
						wms_asn_create_purchase_receipt(frm, warehouse);
						return;
					}
					wms_asn_prompt_warehouse(frm, (selected) => {
						wms_asn_create_purchase_receipt(frm, selected);
					});
				}
			);
		}, __("Actions"));
	},
});
