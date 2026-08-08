// WMS Cycle Count Batch — linked tasks panel + reconciliation workflow

const WMS_BATCH_TASKS_PANEL = ".wms-batch-tasks-panel";

function wms_remove_batch_tasks_panel(frm) {
	frm.$wrapper.find(WMS_BATCH_TASKS_PANEL).remove();
}

function wms_status_indicator(status) {
	const map = {
		Posted: "green",
		Completed: "blue",
		Validated: "blue",
		"In Progress": "orange",
		Open: "grey",
		Cancelled: "red",
		Closed: "grey",
	};
	return map[status] || "grey";
}

function wms_paint_batch_tasks_panel(frm, $panel, data) {
	const batch_status = data.batch_status || frm.doc.status || "Draft";
	const editable = batch_status !== "Posted";
	const tasks = data.tasks || [];

	let rows_html = "";
	if (!tasks.length) {
		rows_html = `<tr><td colspan="7" class="text-muted">${__("No linked count tasks yet. Mobile pushes will appear here.")}</td></tr>`;
	} else {
		for (const task of tasks) {
			const is_posted = task.status === "Posted";
			const checked = task.include_in_post ? "checked" : "";
			const disabled = !editable || is_posted ? "disabled" : "";
			const row_class = is_posted ? "text-muted" : "";
			const task_link = frappe.utils.get_form_link("WMS Cycle Count Task", task.name, true);
			const include_cell = is_posted
				? `<span class="indicator-pill ${task.include_in_post ? "green" : "grey"}">${task.include_in_post ? __("Yes") : __("No")}</span>`
				: `<input type="checkbox" class="wms-task-include" data-task="${frappe.utils.escape_html(task.name)}" ${checked} ${disabled}>`;

			rows_html += `
				<tr class="${row_class}" data-task="${frappe.utils.escape_html(task.name)}">
					<td style="width:40px;text-align:center;">${include_cell}</td>
					<td>${task_link}</td>
					<td>${frappe.datetime.str_to_user(task.posting_date) || "-"}</td>
					<td><span class="indicator-pill ${wms_status_indicator(task.status)}">${__(task.status || "-")}</span></td>
					<td>${__(task.count_mode || "-")}</td>
					<td class="text-right">${task.line_count || 0}</td>
					<td>${frappe.utils.escape_html(task.external_ref || "-")}</td>
				</tr>`;
		}
	}

	const toolbar = editable
		? `<div class="flex justify-between align-center" style="margin-bottom:8px;">
				<div class="text-muted">${__("{0} task(s) linked · {1} included for post · {2} pending", [
					data.total || 0,
					data.included_count || 0,
					data.pending_count || 0,
				])}</div>
				<div>
					<button type="button" class="btn btn-xs btn-default wms-select-all-tasks">${__("Select All Pending")}</button>
					<button type="button" class="btn btn-xs btn-default wms-clear-all-tasks">${__("Clear Selection")}</button>
				</div>
			</div>`
		: `<div class="text-muted" style="margin-bottom:8px;">${__("{0} linked task(s)", [data.total || 0])}</div>`;

	$panel.html(`
		<div class="section-head collapsible">
			<span>${__("Linked Count Tasks")}</span>
		</div>
		<div class="section-body">
			<p class="text-muted small">${__(
				"Check tasks to include in Preview and Post. Posted tasks are skipped automatically. Different posting dates can share one batch."
			)}</p>
			${toolbar}
			<div class="table-responsive">
				<table class="table table-bordered table-sm wms-batch-tasks-table">
					<thead>
						<tr>
							<th>${__("Include")}</th>
							<th>${__("Task")}</th>
							<th>${__("Posting Date")}</th>
							<th>${__("Status")}</th>
							<th>${__("Count Mode")}</th>
							<th class="text-right">${__("Lines")}</th>
							<th>${__("External Ref")}</th>
						</tr>
					</thead>
					<tbody>${rows_html}</tbody>
				</table>
			</div>
		</div>
	`);

	$panel.find(".wms-task-include").on("change", function () {
		const task_name = $(this).data("task");
		const include = $(this).is(":checked") ? 1 : 0;
		wms_save_batch_task_inclusion(frm, [{ name: task_name, include_in_post: include }], $panel);
	});

	$panel.find(".wms-select-all-tasks").on("click", () => {
		const selections = tasks
			.filter((t) => t.status !== "Posted")
			.map((t) => ({ name: t.name, include_in_post: 1 }));
		if (!selections.length) return;
		wms_save_batch_task_inclusion(frm, selections, $panel, true);
	});

	$panel.find(".wms-clear-all-tasks").on("click", () => {
		const selections = tasks
			.filter((t) => t.status !== "Posted")
			.map((t) => ({ name: t.name, include_in_post: 0 }));
		if (!selections.length) return;
		wms_save_batch_task_inclusion(frm, selections, $panel, true);
	});
}

function wms_save_batch_task_inclusion(frm, selections, $panel, reload_panel = false) {
	frappe.call({
		method: "printechs_wms.api.cycle_count_batch.set_batch_task_inclusion",
		args: {
			batch_name: frm.doc.name,
			selections: JSON.stringify(selections),
		},
		freeze: true,
		freeze_message: __("Saving task selection..."),
		callback: (r) => {
			const m = r.message || {};
			if (!m.ok) return;

			if (m.batch_reset) {
				frappe.show_alert({
					message: __("Batch reset to Draft — run Preview again before posting."),
					indicator: "orange",
				});
				frm.reload_doc();
				return;
			}

			if (reload_panel) {
				wms_paint_batch_tasks_panel(frm, $panel, m);
				frappe.show_alert({ message: __("Task selection updated."), indicator: "green" });
				return;
			}

			const $summary = $panel.find(".text-muted").first();
			if ($summary.length) {
				$summary.text(
					__("{0} task(s) linked · {1} included for post · {2} pending", [
						m.total || 0,
						m.included_count || 0,
						m.pending_count || 0,
					])
				);
			}
		},
	});
}

function wms_render_batch_tasks_panel(frm) {
	wms_remove_batch_tasks_panel(frm);
	if (frm.is_new()) return;

	const $anchor = frm.fields_dict.summary && frm.fields_dict.summary.$wrapper;
	if (!$anchor || !$anchor.length) return;

	const $panel = $(`<div class="${WMS_BATCH_TASKS_PANEL.slice(1)} form-section"></div>`);
	$panel.insertBefore($anchor);
	$panel.html(`<div class="text-muted">${__("Loading linked tasks...")}</div>`);

	frappe.call({
		method: "printechs_wms.api.cycle_count_batch.get_batch_linked_tasks",
		args: { batch_name: frm.doc.name },
		callback: (r) => {
			const m = r.message || {};
			if (!m.ok) {
				$panel.html(`<div class="text-danger">${__("Could not load linked tasks.")}</div>`);
				return;
			}
			wms_paint_batch_tasks_panel(frm, $panel, m);
		},
	});
}

frappe.ui.form.on("WMS Cycle Count Batch", {
	refresh(frm) {
		const status = frm.doc.status || "Draft";
		const isDraft = frm.doc.docstatus === 0;

		wms_render_batch_tasks_panel(frm);

		if (!isDraft || frm.is_new()) return;

		if (status !== "Posted") {
			frm.add_custom_button(__("Load Actual Stock Preview"), () => {
				frappe.call({
					method: "printechs_wms.api.cycle_count_batch.load_actual_stock_preview",
					args: { batch_name: frm.doc.name },
					freeze: true,
					callback: (r) => {
						const m = r.message || {};
						if (m.ok) {
							frappe.msgprint(
								`Preview done. Updated lines: ${m.updated_lines}, Summary rows: ${m.summary_rows}`
							);
							frm.reload_doc();
						}
					},
				});
			});
		}

		if (status === "Previewed") {
			frm.add_custom_button(__("Export Verification Excel"), () => {
				frappe.call({
					method: "printechs_wms.api.cycle_count_batch.export_opening_valuation_template",
					args: { batch_name: frm.doc.name },
					freeze: true,
					freeze_message: __("Generating Excel..."),
					callback: (r) => {
						const m = r.message || {};
						if (!m.ok) {
							frappe.msgprint({
								title: __("Error"),
								message: m.message || __("Export failed."),
								indicator: "red",
							});
							return;
						}
						const missing = m.missing_rate_count
							? `<br>Missing rates: <b>${m.missing_rate_count}</b>`
							: "";
						frappe.msgprint({
							title: __("Verification Excel"),
							message: `Items: <b>${m.item_count}</b>${missing}<br>File: <a href="${m.file_url}" target="_blank">${m.file_name}</a>`,
							indicator: "green",
						});
					},
				});
			}).addClass("btn-primary");

			frm.add_custom_button(__("Upload Verified Excel (Create Reconciliation SR)"), () => {
				const company = frm.doc.company || "";
				frappe.prompt(
					[
						{
							fieldname: "valuation_file",
							fieldtype: "Attach",
							label: __("Verification Excel"),
							reqd: 1,
							description: __(
								"Upload finance-verified Excel. counted_qty must not be changed — valuation_rate only."
							),
						},
						{
							fieldname: "difference_account",
							fieldtype: "Link",
							options: "Account",
							label: __("Difference Account"),
							reqd: 0,
							get_query: () => ({
								filters: [
									["Account", "root_type", "=", "Expense"],
									["Account", "is_group", "=", 0],
									["Account", "company", "=", company],
								],
							}),
						},
					],
					(values) => {
						if (!values || !values.valuation_file) return;
						frappe.call({
							method: "printechs_wms.api.cycle_count_batch.upload_opening_valuation_file",
							args: {
								file_url: values.valuation_file,
								batch_name: frm.doc.name,
								difference_account: values.difference_account,
							},
							freeze: true,
							freeze_message: __("Creating Stock Reconciliation..."),
							callback: (r) => {
								const m = r.message || {};
								if (!m.ok) {
									frappe.msgprint({
										title: __("Error"),
										message: m.message || __("Upload failed."),
										indicator: "red",
									});
									return;
								}
								frappe.msgprint({
									title: __("Stock Reconciliation Created"),
									message: `SR: <b>${m.sr}</b><br>Rows in SR: <b>${m.row_count_in_sr}</b>`,
									indicator: "green",
								});
								frm.reload_doc();
							},
						});
					},
					__("Upload Verified Excel"),
					__("Upload & Create SR")
				);
			});

			frm.add_custom_button(__("Confirm & Post Batch"), () => {
				frappe.confirm(__("Update WMS Stock Balance for this batch?"), () => {
					frappe.call({
						method: "printechs_wms.api.cycle_count_batch.confirm_and_post_batch",
						args: { batch_name: frm.doc.name, create_stock_reconciliation: 0 },
						freeze: true,
						callback: (r) => {
							const m = r.message || {};
							if (!m.ok) {
								frappe.msgprint({
									title: __("Error"),
									message: m.message || __("Posting failed."),
									indicator: "red",
								});
								return;
							}
							frappe.msgprint({
								title: __("Posted"),
								message: `Updated balances: <b>${m.updated_balances}</b><br>Cleared stale cartons: <b>${m.cleared_stale_cartons || 0}</b><br>SR: <b>${m.sr || "N/A"}</b>${
									m.sr_note ? `<br><span class="text-muted">${m.sr_note}</span>` : ""
								}`,
								indicator: "green",
							});
							frm.reload_doc();
						},
					});
				});
			}).addClass("btn-danger");
		}
	},
});
