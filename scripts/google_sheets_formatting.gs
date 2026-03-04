/**
 * OSHA sales sheet formatting automation.
 *
 * Usage:
 * 1) Open the target Google Sheet.
 * 2) Extensions -> Apps Script.
 * 3) Paste this file, save, then run `applySalesFormatting` once.
 * 4) Run `installHourlyFormattingTrigger` once to keep styles reapplied.
 */

const OSHA_SHEET_FORMAT_CONFIG = {
  headerRow: 1,
  targetSheetNames: [
    "Bay Area OSHA Follow-Ups",
    "SoCal OSHA Follow-Ups",
  ],
  targetSheetNameContains: "OSHA Follow-Ups",
};

function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu("OSHA Sales")
    .addItem("Apply Sales Formatting", "applySalesFormatting")
    .addItem("Install Hourly Formatting Trigger", "installHourlyFormattingTrigger")
    .addItem("Remove Formatting Triggers", "removeFormattingTriggers")
    .addToUi();
}

function applySalesFormatting() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheets = resolveTargetSheets_(ss);
  sheets.forEach((sheet) => {
    try {
      formatSalesSheet_(sheet);
    } catch (err) {
      Logger.log("Formatting failed for sheet %s: %s", sheet.getName(), err);
    }
  });
}

function installHourlyFormattingTrigger() {
  removeFormattingTriggers();
  ScriptApp.newTrigger("applySalesFormatting")
    .timeBased()
    .everyHours(1)
    .create();
}

function removeFormattingTriggers() {
  const triggers = ScriptApp.getProjectTriggers();
  triggers.forEach((trigger) => {
    if (trigger.getHandlerFunction() === "applySalesFormatting") {
      ScriptApp.deleteTrigger(trigger);
    }
  });
}

function resolveTargetSheets_(ss) {
  const configured = OSHA_SHEET_FORMAT_CONFIG.targetSheetNames
    .map((name) => ss.getSheetByName(name))
    .filter((sheet) => sheet !== null);
  if (configured.length > 0) {
    return configured;
  }

  const all = ss.getSheets();
  const marker = OSHA_SHEET_FORMAT_CONFIG.targetSheetNameContains.toLowerCase();
  return all.filter((sheet) => sheet.getName().toLowerCase().indexOf(marker) >= 0);
}

function formatSalesSheet_(sheet) {
  const headerRow = OSHA_SHEET_FORMAT_CONFIG.headerRow;
  const lastRow = sheet.getLastRow();
  const lastCol = sheet.getLastColumn();
  if (lastRow <= headerRow || lastCol < 1) {
    return;
  }

  const headerRange = sheet.getRange(headerRow, 1, 1, lastCol);
  const headers = headerRange
    .getValues()[0]
    .map((h) => String(h).trim().toLowerCase());

  const colIndex = {};
  headers.forEach((name, idx) => {
    if (name) {
      colIndex[name] = idx + 1;
    }
  });

  // Keep table UX consistent.
  sheet.setFrozenRows(headerRow);
  headerRange.setFontWeight("bold");
  headerRange.setBackground("#0B1F3A");
  headerRange.setFontColor("#FFFFFF");

  const dataRange = sheet.getRange(headerRow + 1, 1, lastRow - headerRow, lastCol);
  if (!sheet.getFilter()) {
    sheet.getRange(headerRow, 1, lastRow - headerRow + 1, lastCol).createFilter();
  }
  dataRange.setWrapStrategy(SpreadsheetApp.WrapStrategy.WRAP);

  // Number/date formats where columns exist.
  setFormatIfPresent_(sheet, headerRow, lastRow, colIndex, "penalties_total_usd", "$#,##0.00");
  setFormatIfPresent_(sheet, headerRow, lastRow, colIndex, "latest_case_close_date", "yyyy-mm-dd");
  setFormatIfPresent_(sheet, headerRow, lastRow, colIndex, "last_violation_event_date", "yyyy-mm-dd");
  setFormatIfPresent_(sheet, headerRow, lastRow, colIndex, "last_accident_date", "yyyy-mm-dd");

  const rules = buildManagedRules_(sheet, colIndex, headerRow, lastRow, lastCol);
  const existing = sheet
    .getConditionalFormatRules()
    .filter((rule) => !isManagedRule_(rule));
  sheet.setConditionalFormatRules(existing.concat(rules));
}

function buildManagedRules_(sheet, colIndex, headerRow, lastRow, lastCol) {
  const dataRange = sheet.getRange(headerRow + 1, 1, lastRow - headerRow, lastCol);
  const rules = [];

  const priorityCol = colIndex["followup_priority"];
  if (priorityCol) {
    const pCol = columnToLetter_(priorityCol);
    rules.push(
      SpreadsheetApp.newConditionalFormatRule()
        .whenFormulaSatisfied(`=AND(N("OSHA_RULE_P1"),$${pCol}${headerRow + 1}="Priority 1")`)
        .setBackground("#FDE8E7")
        .setRanges([dataRange])
        .build()
    );
    rules.push(
      SpreadsheetApp.newConditionalFormatRule()
        .whenFormulaSatisfied(`=AND(N("OSHA_RULE_P2"),$${pCol}${headerRow + 1}="Priority 2")`)
        .setBackground("#FFF4D6")
        .setRanges([dataRange])
        .build()
    );
    rules.push(
      SpreadsheetApp.newConditionalFormatRule()
        .whenFormulaSatisfied(`=AND(N("OSHA_RULE_P3"),$${pCol}${headerRow + 1}="Priority 3")`)
        .setBackground("#EAF7EE")
        .setRanges([dataRange])
        .build()
    );
  }

  const severeCol = colIndex["severe_incident_signal"];
  if (severeCol) {
    const sCol = columnToLetter_(severeCol);
    rules.push(
      SpreadsheetApp.newConditionalFormatRule()
        .whenFormulaSatisfied(`=AND(N("OSHA_RULE_SEVERE"),$${sCol}${headerRow + 1}="Yes")`)
        .setFontColor("#9B1C1C")
        .setBold(true)
        .setRanges([dataRange])
        .build()
    );
  }

  const openCol = colIndex["has_open_violations"];
  if (openCol) {
    const oCol = columnToLetter_(openCol);
    rules.push(
      SpreadsheetApp.newConditionalFormatRule()
        .whenFormulaSatisfied(`=AND(N("OSHA_RULE_OPEN"),$${oCol}${headerRow + 1}="Yes")`)
        .setFontColor("#B42318")
        .setRanges([dataRange])
        .build()
    );
  }

  return rules;
}

function isManagedRule_(rule) {
  const condition = rule.getBooleanCondition();
  if (!condition) {
    return false;
  }
  if (condition.getCriteriaType() !== SpreadsheetApp.BooleanCriteria.CUSTOM_FORMULA) {
    return false;
  }
  const values = condition.getCriteriaValues() || [];
  return values.some((v) => String(v).indexOf("OSHA_RULE_") >= 0);
}

function setFormatIfPresent_(sheet, headerRow, lastRow, colIndex, columnName, formatPattern) {
  const idx = colIndex[columnName];
  if (!idx) {
    return;
  }
  sheet
    .getRange(headerRow + 1, idx, lastRow - headerRow, 1)
    .setNumberFormat(formatPattern);
}

function columnToLetter_(column) {
  let temp = "";
  let col = column;
  while (col > 0) {
    const rem = (col - 1) % 26;
    temp = String.fromCharCode(65 + rem) + temp;
    col = Math.floor((col - rem - 1) / 26);
  }
  return temp;
}
