/* eslint-disable */

frappe.query_reports["WMS Stock Ledger Entry"] = {
  filters: [
    {
      fieldname: "item_code",
      label: __("Item Code"),
      fieldtype: "Link",
      options: "Item",
      on_change: function () {
        apply_rules(true);
      },
    },
    { fieldname: "event_type", label: __("Event Type"), fieldtype: "Data" },

    {
      fieldname: "location",
      label: __("Location"),
      fieldtype: "Data",
      on_change: function () {
        apply_rules(true);
      },
    },

    { fieldname: "carton", label: __("Carton"), fieldtype: "Data" },

    {
      fieldname: "from_date",
      label: __("From Datetime"),
      fieldtype: "Datetime",
    },
    { fieldname: "to_date", label: __("To Datetime"), fieldtype: "Datetime" },

    {
      fieldname: "show_full_log",
      label: __("Show Full Log"),
      fieldtype: "Check",
      default: 1,
    },

    {
      fieldname: "group_by",
      label: __("Group By"),
      fieldtype: "Select",
      options: "\nItem\nLocation\nItem + Location",
      on_change: function () {
        apply_rules(true);
      },
    },

    { fieldname: "limit", label: __("Limit"), fieldtype: "Int", default: 500 },
  ],

  onload: function (report) {
    // apply once when report loads
    apply_rules(false, report);
  },

  refresh: function (report) {
    // IMPORTANT: filters area gets re-rendered, so apply again
    apply_rules(false, report);
  },
};

// ------------------------------------------------------------
// RULES
// ------------------------------------------------------------
function apply_rules(clear_conflicts, report) {
  report = report || frappe.query_report;

  const item_code = report.get_filter_value("item_code");
  const group_by = (report.get_filter_value("group_by") || "").trim();

  // Helper: show/hide filter fields (v15 correct method)
  function show(fieldname, visible) {
    // visible=true => show; visible=false => hide
    report.toggle_filter_display(fieldname, !!visible);
  }

  // 1) If item_code selected => group_by must be Item, and hide location+carton
  if (item_code) {
    if (group_by !== "Item") {
      report.set_filter_value("group_by", "Item");
    }

    if (clear_conflicts) {
      report.set_filter_value("location", "");
      report.set_filter_value("carton", "");
    }

    show("location", false);
    show("carton", false);
    return;
  }

  // 2) If group_by = Item => hide location+carton
  if (group_by === "Item") {
    if (clear_conflicts) {
      report.set_filter_value("location", "");
      report.set_filter_value("carton", "");
    }
    show("location", false);
    show("carton", false);
    return;
  }

  // 3) If group_by = Location => show location, hide carton
  if (group_by === "Location") {
    if (clear_conflicts) {
      report.set_filter_value("carton", "");
    }
    show("location", true);
    show("carton", false);
    return;
  }

  // 4) If group_by = Item + Location => show location, hide carton (recommended)
  if (group_by === "Item + Location") {
    if (clear_conflicts) {
      report.set_filter_value("carton", "");
    }
    show("location", true);
    show("carton", false); // if you want carton visible here -> change to true
    return;
  }

  // 5) Default => show all
  show("location", true);
  show("carton", true);
}
