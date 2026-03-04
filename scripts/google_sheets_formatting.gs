/**
 * OSHA sales sheet formatting automation (Connected Sheets safe).
 *
 * Why this version:
 * - Connected Sheets DATASOURCE tabs block some formatting APIs.
 * - This script creates/refreshes normal "styled mirror" tabs from datasource tabs.
 * - Formatting is applied to mirror tabs, not datasource tabs.
 */

const OSHA_SHEET_FORMAT_CONFIG = {
  headerRow: 1,

  // Source Connected Sheets tabs.
  sourceDataSheetNames: [
    "v_sales_followup_bayarea_v2",
    "v_sales_followup_sandiego_v2",
  ],

  // source tab -> styled mirror tab
  mirrorTabBySource: {
    v_sales_followup_bayarea_v2: "Bay Area OSHA Follow-Ups",
    v_sales_followup_sandiego_v2: "SoCal OSHA Follow-Ups",
  },

  // Additional non-datasource tabs you may want formatted.
  directTargetSheetNames: [
    "Bay Area OSHA Follow-Ups",
    "SoCal OSHA Follow-Ups",
    "Bay Area OSHA Follow-Ups (Styled)",
    "SoCal OSHA Follow-Ups (Styled)",
  ],

  // Fallback discovery for non-datasource tabs.
  targetSheetNameContains: "follow-up",
};

function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu("OSHA Sales")
    .addItem("Create/Refresh Styled Mirrors", "createOrRefreshStyledMirrors")
    .addItem("Apply Sales Formatting", "applySalesFormatting")
    .addItem("Install Hourly Formatting Trigger", "installHourlyFormattingTrigger")
    .addItem("Remove Formatting Triggers", "removeFormattingTriggers")
    .addToUi();
}

function createOrRefreshStyledMirrors() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  createOrRefreshStyledMirrors_(ss);
}

function applySalesFormatting() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();

  // Ensure mirrors exist before formatting.
  createOrRefreshStyledMirrors_(ss);

  const targets = resolveFormatTargets_(ss);
  if (targets.length === 0) {
    Logger.log("No non-datasource target sheets found to format.");
    return;
  }

  targets.forEach((sheet) => {
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

function createOrRefreshStyledMirrors_(ss) {
  OSHA_SHEET_FORMAT_CONFIG.sourceDataSheetNames.forEach((sourceName) => {
    const preferredMirror = OSHA_SHEET_FORMAT_CONFIG.mirrorTabBySource[sourceName] || (sourceName + " (Styled)");
    ensureMirrorSheet_(ss, sourceName, preferredMirror);
  });
}

function ensureMirrorSheet_(ss, sourceName, preferredMirrorName) {
  const source = ss.getSheetByName(sourceName);
  if (!source) {
    Logger.log("Source sheet not found: %s", sourceName);
    return null;
  }

  let mirror = ss.getSheetByName(preferredMirrorName);
  if (mirror && isDataSourceSheet_(mirror)) {
    const altName = preferredMirrorName + " (Styled)";
    mirror = ss.getSheetByName(altName) || ss.insertSheet(altName);
  } else if (!mirror) {
    mirror = ss.insertSheet(preferredMirrorName);
  }

  const formula = "=ARRAYFORMULA('" + escapeSheetName_(sourceName) + "'!A:ZZ)";
  const a1 = mirror.getRange(1, 1);
  if (a1.getFormula() !== formula) {
    mirror.clear();
    a1.setFormula(formula);
  }
  return mirror;
}

function resolveFormatTargets_(ss) {
  const byId = {};

  OSHA_SHEET_FORMAT_CONFIG.directTargetSheetNames.forEach((name) => {
    const sheet = ss.getSheetByName(name);
    if (sheet && !isDataSourceSheet_(sheet)) {
      byId[sheet.getSheetId()] = sheet;
    }
  });

  const marker = OSHA_SHEET_FORMAT_CONFIG.targetSheetNameContains.toLowerCase();
  ss.getSheets().forEach((sheet) => {
    const name = sheet.getName().toLowerCase();
    if (name.indexOf(marker) >= 0 && !isDataSourceSheet_(sheet)) {
      byId[sheet.getSheetId()] = sheet;
    }
  });

  return Object.keys(byId).map((id) => byId[id]);
}

function formatSalesSheet_(sheet) {
  const headerRow = OSHA_SHEET_FORMAT_CONFIG.headerRow;
  const lastRow = sheet.getLastRow();
  const lastCol = sheet.getLastColumn();
  if (lastRow <= headerRow || lastCol < 1) {
    return;
  }

  const headerRange = sheet.getRange(headerRow, 1, 1, lastCol);
  const headers = headerRange.getValues()[0].map((h) => normalizeHeader_(h));

  const colIndex = {};
  headers.forEach((name, idx) => {
    if (name) {
      colIndex[name] = idx + 1;
    }
  });

  // Base visual style.
  sheet.setFrozenRows(headerRow);
  headerRange.setFontWeight("bold");
  headerRange.setBackground("#0B1F3A");
  headerRange.setFontColor("#FFFFFF");

  const dataRange = sheet.getRange(headerRow + 1, 1, lastRow - headerRow, lastCol);
  if (!sheet.getFilter()) {
    sheet.getRange(headerRow, 1, lastRow - headerRow + 1, lastCol).createFilter();
  }
  dataRange.setWrapStrategy(SpreadsheetApp.WrapStrategy.WRAP);

  // Number/date formats.
  setFormatIfPresent_(sheet, headerRow, lastRow, colIndex, "penalties_total_usd", "$#,##0.00");
  setFormatIfPresent_(sheet, headerRow, lastRow, colIndex, "latest_case_close_date", "yyyy-mm-dd");
  setFormatIfPresent_(sheet, headerRow, lastRow, colIndex, "last_violation_event_date", "yyyy-mm-dd");
  setFormatIfPresent_(sheet, headerRow, lastRow, colIndex, "last_accident_date", "yyyy-mm-dd");

  const managedRules = buildManagedRules_(sheet, colIndex, headerRow, lastRow, lastCol);
  const existing = sheet.getConditionalFormatRules().filter((r) => !isManagedRule_(r));
  sheet.setConditionalFormatRules(existing.concat(managedRules));
}

function buildManagedRules_(sheet, colIndex, headerRow, lastRow, lastCol) {
  const dataRange = sheet.getRange(headerRow + 1, 1, lastRow - headerRow, lastCol);
  const rules = [];

  const priorityCol = colIndex["followup_priority"];
  if (priorityCol) {
    const pCol = columnToLetter_(priorityCol);
    rules.push(
      SpreadsheetApp.newConditionalFormatRule()
        .whenFormulaSatisfied('=AND(N("OSHA_RULE_P1"),$' + pCol + (headerRow + 1) + '="Priority 1")')
        .setBackground("#FDE8E7")
        .setRanges([dataRange])
        .build()
    );
    rules.push(
      SpreadsheetApp.newConditionalFormatRule()
        .whenFormulaSatisfied('=AND(N("OSHA_RULE_P2"),$' + pCol + (headerRow + 1) + '="Priority 2")')
        .setBackground("#FFF4D6")
        .setRanges([dataRange])
        .build()
    );
    rules.push(
      SpreadsheetApp.newConditionalFormatRule()
        .whenFormulaSatisfied('=AND(N("OSHA_RULE_P3"),$' + pCol + (headerRow + 1) + '="Priority 3")')
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
        .whenFormulaSatisfied('=AND(N("OSHA_RULE_SEVERE"),$' + sCol + (headerRow + 1) + '="Yes")')
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
        .whenFormulaSatisfied('=AND(N("OSHA_RULE_OPEN"),$' + oCol + (headerRow + 1) + '="Yes")')
        .setFontColor("#B42318")
        .setRanges([dataRange])
        .build()
    );
  }

  const urgencyCol = colIndex["urgency_band"];
  if (urgencyCol) {
    const uCol = columnToLetter_(urgencyCol);
    rules.push(
      SpreadsheetApp.newConditionalFormatRule()
        .whenFormulaSatisfied('=AND(N("OSHA_RULE_URG_RED"),$' + uCol + (headerRow + 1) + '="RED")')
        .setBackground("#FDE8E7")
        .setRanges([dataRange])
        .build()
    );
    rules.push(
      SpreadsheetApp.newConditionalFormatRule()
        .whenFormulaSatisfied('=AND(N("OSHA_RULE_URG_YELLOW"),$' + uCol + (headerRow + 1) + '="YELLOW")')
        .setBackground("#FFF4D6")
        .setRanges([dataRange])
        .build()
    );
    rules.push(
      SpreadsheetApp.newConditionalFormatRule()
        .whenFormulaSatisfied('=AND(N("OSHA_RULE_URG_GREEN"),$' + uCol + (headerRow + 1) + '="GREEN")')
        .setBackground("#EAF7EE")
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

function setFormatIfPresent_(sheet, headerRow, lastRow, colIndex, columnName, pattern) {
  const idx = colIndex[columnName];
  if (!idx || lastRow <= headerRow) {
    return;
  }
  sheet.getRange(headerRow + 1, idx, lastRow - headerRow, 1).setNumberFormat(pattern);
}

function isDataSourceSheet_(sheet) {
  try {
    return sheet.asDataSourceSheet() !== null;
  } catch (err) {
    return false;
  }
}

function normalizeHeader_(value) {
  return String(value)
    .trim()
    .toLowerCase()
    .replace(/\s+/g, "_")
    .replace(/[^\w]/g, "_")
    .replace(/_+$/g, "");
}

function columnToLetter_(column) {
  let result = "";
  let col = column;
  while (col > 0) {
    const rem = (col - 1) % 26;
    result = String.fromCharCode(65 + rem) + result;
    col = Math.floor((col - rem - 1) / 26);
  }
  return result;
}

function escapeSheetName_(name) {
  return String(name).replace(/'/g, "''");
}
