/**
 * OSHA Sales Views (Bay Area + San Diego) - Prescription Safety Eyewear Focus
 *
 * Simplified output columns:
 * Region, Account Name, Site Address, Site City, Site State, Site ZIP,
 * NAICS Code, Industry Segment, Ownership Type, Inspection Type,
 * Latest Case Close Date, Days Since Last Case Close, Employee Count Estimate,
 * Has Open Violations, Penalties Total USD, Program Relevance,
 * Prescription Signal Count, Direct Prescription Citation Count,
 * Follow-up Score, Suggested Action
 *
 * Sorting:
 * 1) Suggested Action: Call within 24 hours -> Call this week -> Nurture this month
 * 2) Follow-up Priority: Priority 1 -> Priority 2 -> Priority 3
 * 3) Program Relevance: Prescription Safety -> General PPE / Eyewear
 * 4) Follow-up Score (desc)
 * 5) Follow-up Percentile (desc, if available)
 * 6) Days Since Last Case Close (asc)
 *
 * Filtering:
 * - Keep Priority 1 + Priority 2 when Follow-up Priority is present
 * - Fallback: keep Follow-up Score >= minFollowupScore
 */

function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu("OSHA Sales Views")
    .addItem("Build Bay Area View", "setupBayAreaOshaViewSalesFriendly")
    .addItem("Build San Diego View", "setupSanDiegoOshaViewSalesFriendly")
    .addItem("Build Both Views", "setupAllOshaViewsSalesFriendly")
    .addSeparator()
    .addItem("Install Hourly Refresh", "installHourlyOshaViewTriggers")
    .addItem("Install Daily 9 AM Refresh", "installDailyOshaViewTriggersAt9am")
    .addItem("Remove Refresh Triggers", "removeOshaViewTriggers")
    .addToUi();
}

function setupBayAreaOshaViewSalesFriendly() {
  setupRegionalOshaViewSalesFriendly_({
    dataSourceTabName: "v_sales_followup_bayarea_v2",
    extractTabName: "v_sales_followup_bayarea_v2_extract",
    viewTabName: "BayArea_View",
    minFollowupScore: 55
  });
}

function setupSanDiegoOshaViewSalesFriendly() {
  setupRegionalOshaViewSalesFriendly_({
    dataSourceTabName: "v_sales_followup_sandiego_v2",
    extractTabName: "v_sales_followup_sandiego_v2_extract",
    viewTabName: "SanDiego_View",
    minFollowupScore: 55
  });
}

function setupAllOshaViewsSalesFriendly() {
  setupBayAreaOshaViewSalesFriendly();
  setupSanDiegoOshaViewSalesFriendly();
}

function installHourlyOshaViewTriggers() {
  replaceTimeTrigger_("setupBayAreaOshaViewSalesFriendly", "hourly");
  replaceTimeTrigger_("setupSanDiegoOshaViewSalesFriendly", "hourly");
}

function installDailyOshaViewTriggersAt9am() {
  replaceTimeTrigger_("setupBayAreaOshaViewSalesFriendly", "daily");
  replaceTimeTrigger_("setupSanDiegoOshaViewSalesFriendly", "daily");
}

function removeOshaViewTriggers() {
  var triggers = ScriptApp.getProjectTriggers();
  for (var i = 0; i < triggers.length; i++) {
    var handler = triggers[i].getHandlerFunction();
    if (
      handler === "setupBayAreaOshaViewSalesFriendly" ||
      handler === "setupSanDiegoOshaViewSalesFriendly"
    ) {
      ScriptApp.deleteTrigger(triggers[i]);
    }
  }
}

function installOshaViewAssets() {
  installDailyOshaViewTriggersAt9am();
  setupAllOshaViewsSalesFriendly();
}

function replaceTimeTrigger_(handlerName, mode) {
  var triggers = ScriptApp.getProjectTriggers();
  for (var i = 0; i < triggers.length; i++) {
    if (triggers[i].getHandlerFunction() === handlerName) {
      ScriptApp.deleteTrigger(triggers[i]);
    }
  }

  var builder = ScriptApp.newTrigger(handlerName).timeBased();
  if (mode === "daily") {
    builder.atHour(9).everyDays(1).create();
    return;
  }
  builder.everyHours(1).create();
}

function setupRegionalOshaViewSalesFriendly_(regionConfig) {
  var BASE_CONFIG = {
    headerRawRow: 1,
    headerPrettyRow: 2,
    dataStartRow: 3,
    refreshTimeoutMs: 300000,
    refreshPollMs: 3000,
    maxRowsToProcess: 50000,
    minFollowupScore: 55,
    freezePrettyHeaderRow: true,
    freezeAccountNameColumn: true,
    defaultColWidth: 140,
    uniformRowHeight: 34,
    penaltiesHighUsd: 10000,
    colorBatchSize: 500,
    displayColumns: [
      { header: "Region" },
      { header: "Account Name" },
      { header: "Site Address" },
      { header: "Site City" },
      { header: "Site State" },
      { header: "Site ZIP" },
      { header: "NAICS Code" },
      { header: "Industry Segment" },
      { header: "Ownership Type" },
      { header: "Inspection Type" },
      { header: "Latest Case Close Date" },
      { header: "Days Since Last Case Close" },
      { header: "Employee Count Estimate" },
      { header: "Has Open Violations" },
      { header: "Penalties Total USD" },
      { header: "Program Relevance", aliases: ["Citation Category"], defaultValue: "" },
      { header: "Prescription Signal Count", required: false, defaultValue: "0" },
      { header: "Direct Prescription Citation Count", required: false, defaultValue: "0" },
      { header: "Follow-up Score" },
      { header: "Suggested Action" }
    ],
    helperColumns: [
      { header: "Follow-up Priority", required: false },
      { header: "Follow-up Percentile", required: false }
    ],
    wrapHeaders: [
      "Account Name",
      "Site Address",
      "Industry Segment",
      "Program Relevance"
    ],
    colorCall24h: "#f4cccc",
    colorCallWeek: "#fff2cc",
    colorNurture: "#d9ead3",
    colorPrescriptionSafety: "#dbeafe",
    colorOpenViolations: "#fff2cc",
    colorHighPenalties: "#fce5cd"
  };

  var CONFIG = Object.assign({}, BASE_CONFIG, regionConfig || {});
  CONFIG.displayHeaders = CONFIG.displayColumns.map(function(spec) { return spec.header; });
  CONFIG.helperHeaders = CONFIG.helperColumns.map(function(spec) { return spec.header; });
  if (!CONFIG.dataSourceTabName || !CONFIG.extractTabName || !CONFIG.viewTabName) {
    throw new Error("Missing required config: dataSourceTabName, extractTabName, or viewTabName.");
  }

  var lock = LockService.getScriptLock();
  if (!lock.tryLock(20000)) {
    throw new Error("Another run is already in progress.");
  }

  try {
    var ss = SpreadsheetApp.getActiveSpreadsheet();
    if (!ss) {
      throw new Error("No active spreadsheet. Use a bound script.");
    }

    refreshConnectedSheet_(ss, CONFIG);

    var extract = ss.getSheetByName(CONFIG.extractTabName);
    if (!extract) {
      throw new Error('Extract tab not found: "' + CONFIG.extractTabName + '".');
    }

    var view = ensureNormalSheet_(ss, CONFIG.viewTabName);
    var sourceLastCol = extract.getLastColumn();
    if (sourceLastCol === 0) {
      throw new Error("Extract has no columns.");
    }

    var sourceHeadersRaw = extract.getRange(1, 1, 1, sourceLastCol).getDisplayValues()[0];
    var sourceColCount = lastNonEmptyCol_(sourceHeadersRaw);
    if (sourceColCount === 0) {
      throw new Error("Extract header row is empty.");
    }

    var sourceHeaders = sourceHeadersRaw.slice(0, sourceColCount).map(normalizeHeaderSpacing_);
    var sourceIdxByKey = buildHeaderIndex_(sourceHeaders);

    var resolvedDisplayColumns = resolveColumnSpecs_(CONFIG.displayColumns, sourceIdxByKey);
    var resolvedHelperColumns = resolveColumnSpecs_(CONFIG.helperColumns, sourceIdxByKey);

    var missingDisplay = resolvedDisplayColumns.filter(function(spec) {
      return spec.required !== false && spec.sourceIndex === null;
    }).map(function(spec) {
      return spec.header;
    });
    if (missingDisplay.length) {
      throw new Error("Missing required display columns in extract: " + missingDisplay.join(", "));
    }

    var selectedColumnSpecs = resolvedDisplayColumns.concat(
      resolvedHelperColumns.filter(function(spec) {
        return spec.sourceIndex !== null;
      })
    );
    var selectedHeaders = selectedColumnSpecs.map(function(spec) {
      return spec.header;
    });

    var sourceLastRow = extract.getLastRow();
    var totalSourceRows = Math.max(0, sourceLastRow - 1);
    var readRows = Math.min(totalSourceRows, CONFIG.maxRowsToProcess);
    var outputRows = [];

    if (readRows > 0) {
      var fullData = extract.getRange(2, 1, readRows, sourceColCount).getDisplayValues();
      var projected = fullData.map(function(row) {
        return selectedColumnSpecs.map(function(spec) {
          if (spec.sourceIndex === null) {
            return spec.defaultValue;
          }
          return row[spec.sourceIndex];
        });
      });

      var selHeaderIdx = buildHeaderIndex_(selectedHeaders);
      var colPriority = selHeaderIdx[normalizeKey_("Follow-up Priority")] || 0;
      var colScore = selHeaderIdx[normalizeKey_("Follow-up Score")] || 0;

      var filtered = [];
      if (colPriority) {
        filtered = projected.filter(function(row) {
          var priority = String(row[colPriority - 1] || "").trim().toLowerCase();
          return priority === "priority 1" || priority === "priority 2";
        });
      } else {
        if (!colScore) {
          throw new Error('Neither "Follow-up Priority" nor "Follow-up Score" was found.');
        }
        filtered = projected.filter(function(row) {
          return parseScore_(row[colScore - 1]) >= CONFIG.minFollowupScore;
        });
      }

      sortRowsForSales_(filtered, selectedHeaders);

      var displayPositions = CONFIG.displayHeaders.map(function(header) {
        return findHeaderCol_(selectedHeaders, header);
      });

      outputRows = filtered.map(function(row) {
        return displayPositions.map(function(pos) {
          return row[pos - 1];
        });
      });
    }

    prepareViewSheet_(view);
    ensureColumns_(view, CONFIG.displayHeaders.length);

    var prettyHeaders = CONFIG.displayHeaders.map(normalizeHeaderSpacing_);
    view.getRange(CONFIG.headerRawRow, 1, 1, CONFIG.displayHeaders.length).setValues([CONFIG.displayHeaders]);
    view.getRange(CONFIG.headerPrettyRow, 1, 1, CONFIG.displayHeaders.length).setValues([prettyHeaders]);

    if (outputRows.length > 0) {
      view.getRange(CONFIG.dataStartRow, 1, outputRows.length, CONFIG.displayHeaders.length).setValues(outputRows);
    }

    applyFrozenPanes_(view, CONFIG, CONFIG.displayHeaders.length);
    view.hideRows(CONFIG.headerRawRow);
    applySalesFriendlyLayout_(view, CONFIG, CONFIG.displayHeaders.length, outputRows.length);
    applyRowColorsFromData_(view, CONFIG, CONFIG.displayHeaders.length, outputRows.length);
    createTableFilter_(view, CONFIG, CONFIG.displayHeaders.length);
  } finally {
    lock.releaseLock();
  }
}

function refreshConnectedSheet_(ss, CONFIG) {
  var dsTab = ss.getSheetByName(CONFIG.dataSourceTabName);
  if (!dsTab) {
    throw new Error('Connected Sheet tab not found: "' + CONFIG.dataSourceTabName + '".');
  }

  if (typeof dsTab.asDataSourceSheet !== "function") {
    return;
  }

  var dsSheet = dsTab.asDataSourceSheet();
  if (!dsSheet) {
    return;
  }

  SpreadsheetApp.enableBigQueryExecution();

  try {
    dsSheet.refreshData();
  } catch (err) {
    // Ignore if the refresh is already running; the wait loop below will handle it.
  }

  var timeoutSeconds = Math.ceil(CONFIG.refreshTimeoutMs / 1000);
  if (typeof ss.waitForAllDataExecutionsCompletion === "function") {
    ss.waitForAllDataExecutionsCompletion(timeoutSeconds);
    SpreadsheetApp.flush();
    return;
  }

  var deadline = Date.now() + CONFIG.refreshTimeoutMs;
  while (Date.now() < deadline) {
    var status = dsSheet.getStatus ? dsSheet.getStatus() : null;
    var state = String(status && status.getExecutionState ? status.getExecutionState() : "").toUpperCase();
    var errCode = String(status && status.getErrorCode ? status.getErrorCode() : "").toUpperCase();

    if (state === "SUCCEEDED" || state === "SUCCESS" || state === "COMPLETED") {
      return;
    }
    if (state === "FAILED" || state === "CANCELLED") {
      throw new Error("Connected Sheet refresh failed (" + state + ")" + (errCode && errCode !== "NONE" ? ": " + errCode : ""));
    }
    Utilities.sleep(CONFIG.refreshPollMs);
  }

  throw new Error("Timed out waiting for Connected Sheet refresh.");
}

function prepareViewSheet_(view) {
  var existing = view.getFilter();
  if (existing) {
    existing.remove();
  }

  var lastRow = view.getLastRow();
  var lastCol = view.getLastColumn();
  if (lastRow > 0 && lastCol > 0) {
    view.getRange(1, 1, lastRow, lastCol).clearContent().clearFormat().clearNote();
  }
}

function applyFrozenPanes_(sheet, CONFIG, colCount) {
  var frozenRows = CONFIG.freezePrettyHeaderRow ? CONFIG.headerPrettyRow : 0;
  sheet.setFrozenRows(Math.max(0, frozenRows));

  var frozenCols = 1;
  if (CONFIG.freezeAccountNameColumn && colCount > 0) {
    var headers = sheet.getRange(CONFIG.headerPrettyRow, 1, 1, colCount).getDisplayValues()[0];
    var idx = buildHeaderIndex_(headers);
    frozenCols = idx[normalizeKey_("Account Name")] || 1;
  }
  sheet.setFrozenColumns(Math.max(1, frozenCols));
}

function applySalesFriendlyLayout_(sheet, CONFIG, colCount, dataRowCount) {
  var headerRange = sheet.getRange(CONFIG.headerPrettyRow, 1, 1, colCount);
  headerRange
    .setFontWeight("bold")
    .setWrap(true)
    .setVerticalAlignment("middle")
    .setBackground("#ffffff");

  sheet.setColumnWidths(1, colCount, CONFIG.defaultColWidth);

  var headers = headerRange.getDisplayValues()[0];
  var idx = buildHeaderIndex_(headers);

  setWidthByHeader_(sheet, idx, "Region", 90);
  setWidthByHeader_(sheet, idx, "Account Name", 300);
  setWidthByHeader_(sheet, idx, "Site Address", 320);
  setWidthByHeader_(sheet, idx, "Site City", 130);
  setWidthByHeader_(sheet, idx, "Site State", 70);
  setWidthByHeader_(sheet, idx, "Site ZIP", 90);
  setWidthByHeader_(sheet, idx, "NAICS Code", 110);
  setWidthByHeader_(sheet, idx, "Industry Segment", 260);
  setWidthByHeader_(sheet, idx, "Ownership Type", 130);
  setWidthByHeader_(sheet, idx, "Inspection Type", 170);
  setWidthByHeader_(sheet, idx, "Latest Case Close Date", 130);
  setWidthByHeader_(sheet, idx, "Days Since Last Case Close", 130);
  setWidthByHeader_(sheet, idx, "Employee Count Estimate", 140);
  setWidthByHeader_(sheet, idx, "Has Open Violations", 130);
  setWidthByHeader_(sheet, idx, "Penalties Total USD", 140);
  setWidthByHeader_(sheet, idx, "Program Relevance", 180);
  setWidthByHeader_(sheet, idx, "Prescription Signal Count", 150);
  setWidthByHeader_(sheet, idx, "Direct Prescription Citation Count", 180);
  setWidthByHeader_(sheet, idx, "Follow-up Score", 120);
  setWidthByHeader_(sheet, idx, "Suggested Action", 170);

  if (dataRowCount <= 0) {
    return;
  }

  var dataRange = sheet.getRange(CONFIG.dataStartRow, 1, dataRowCount, colCount);
  dataRange
    .setWrapStrategy(SpreadsheetApp.WrapStrategy.CLIP)
    .setVerticalAlignment("top");

  for (var i = 0; i < CONFIG.wrapHeaders.length; i++) {
    var wrapCol = idx[normalizeKey_(CONFIG.wrapHeaders[i])];
    if (wrapCol) {
      sheet.getRange(CONFIG.dataStartRow, wrapCol, dataRowCount, 1)
        .setWrapStrategy(SpreadsheetApp.WrapStrategy.WRAP)
        .setVerticalAlignment("top");
    }
  }

  sheet.setRowHeightsForced(CONFIG.dataStartRow, dataRowCount, CONFIG.uniformRowHeight);

  setNumberFormatIfExists_(sheet, idx, CONFIG, "Latest Case Close Date", "yyyy-mm-dd");
  setNumberFormatIfExists_(sheet, idx, CONFIG, "Days Since Last Case Close", "0");
  setNumberFormatIfExists_(sheet, idx, CONFIG, "Employee Count Estimate", "#,##0");
  setNumberFormatIfExists_(sheet, idx, CONFIG, "Penalties Total USD", "$#,##0");
  setNumberFormatIfExists_(sheet, idx, CONFIG, "Prescription Signal Count", "0");
  setNumberFormatIfExists_(sheet, idx, CONFIG, "Direct Prescription Citation Count", "0");
  setNumberFormatIfExists_(sheet, idx, CONFIG, "Follow-up Score", "0");
}

function applyRowColorsFromData_(sheet, CONFIG, colCount, dataRowCount) {
  if (dataRowCount <= 0) {
    return;
  }

  var headers = sheet.getRange(CONFIG.headerPrettyRow, 1, 1, colCount).getDisplayValues()[0];
  var idx = buildHeaderIndex_(headers);
  var colAction = idx[normalizeKey_("Suggested Action")] || 0;
  var colOpen = idx[normalizeKey_("Has Open Violations")] || 0;
  var colPen = idx[normalizeKey_("Penalties Total USD")] || 0;
  var colProgram = idx[normalizeKey_("Program Relevance")] || 0;

  var values = sheet.getRange(CONFIG.dataStartRow, 1, dataRowCount, colCount).getDisplayValues();

  function toNumber(value) {
    var num = Number(String(value || "").replace(/[^0-9.-]/g, ""));
    return isNaN(num) ? 0 : num;
  }

  function getRowColor(row) {
    var action = colAction ? String(row[colAction - 1] || "").trim().toLowerCase() : "";
    var hasOpen = colOpen ? String(row[colOpen - 1] || "").trim().toLowerCase() === "yes" : false;
    var penalties = colPen ? toNumber(row[colPen - 1]) : 0;
    var program = colProgram ? String(row[colProgram - 1] || "").trim().toLowerCase() : "";

    if (action === "call within 24 hours") {
      return CONFIG.colorCall24h;
    }
    if (action === "call this week") {
      return CONFIG.colorCallWeek;
    }
    if (program === "prescription safety") {
      return CONFIG.colorPrescriptionSafety;
    }
    if (action === "nurture this month") {
      return CONFIG.colorNurture;
    }
    if (hasOpen) {
      return CONFIG.colorOpenViolations;
    }
    if (penalties >= CONFIG.penaltiesHighUsd) {
      return CONFIG.colorHighPenalties;
    }
    return "#ffffff";
  }

  var batchSize = CONFIG.colorBatchSize || 500;
  for (var start = 0; start < dataRowCount; start += batchSize) {
    var size = Math.min(batchSize, dataRowCount - start);
    var rows = values.slice(start, start + size);
    var backgrounds = rows.map(function(row) {
      return new Array(colCount).fill(getRowColor(row));
    });
    sheet.getRange(CONFIG.dataStartRow + start, 1, size, colCount).setBackgrounds(backgrounds);
  }

  sheet.getRange(CONFIG.headerPrettyRow, 1, 1, colCount)
    .setBackground("#ffffff")
    .setFontWeight("bold");
}

function createTableFilter_(sheet, CONFIG, colCount) {
  var existing = sheet.getFilter();
  if (existing) {
    existing.remove();
  }

  var lastRow = sheet.getLastRow();
  var rowCount = Math.max(1, lastRow - CONFIG.headerPrettyRow + 1);
  sheet.getRange(CONFIG.headerPrettyRow, 1, rowCount, colCount).createFilter();
}

function sortRowsForSales_(rows, headers) {
  if (!rows || rows.length === 0) {
    return;
  }

  var idx = buildHeaderIndex_(headers);
  var colAction = idx[normalizeKey_("Suggested Action")] || 0;
  var colPriority = idx[normalizeKey_("Follow-up Priority")] || 0;
  var colProgram = idx[normalizeKey_("Program Relevance")] || 0;
  var colScore = idx[normalizeKey_("Follow-up Score")] || 0;
  var colPct = idx[normalizeKey_("Follow-up Percentile")] || 0;
  var colDays = idx[normalizeKey_("Days Since Last Case Close")] || 0;

  function actionRank(value) {
    var normalized = String(value || "").trim().toLowerCase();
    if (normalized === "call within 24 hours") return 1;
    if (normalized === "call this week") return 2;
    if (normalized === "nurture this month") return 3;
    return 4;
  }

  function priorityRank(value) {
    var normalized = String(value || "").trim().toLowerCase();
    if (normalized === "priority 1") return 1;
    if (normalized === "priority 2") return 2;
    if (normalized === "priority 3") return 3;
    return 4;
  }

  function programRank(value) {
    var normalized = String(value || "").trim().toLowerCase();
    if (normalized === "prescription safety") return 1;
    if (normalized === "general ppe / eyewear") return 2;
    return 3;
  }

  function toNum(value) {
    var num = Number(String(value || "").replace(/[^0-9.-]/g, ""));
    return isNaN(num) ? 0 : num;
  }

  rows.sort(function(a, b) {
    if (colAction) {
      var diffAction = actionRank(a[colAction - 1]) - actionRank(b[colAction - 1]);
      if (diffAction !== 0) return diffAction;
    }
    if (colPriority) {
      var diffPriority = priorityRank(a[colPriority - 1]) - priorityRank(b[colPriority - 1]);
      if (diffPriority !== 0) return diffPriority;
    }
    if (colProgram) {
      var diffProgram = programRank(a[colProgram - 1]) - programRank(b[colProgram - 1]);
      if (diffProgram !== 0) return diffProgram;
    }
    if (colScore) {
      var diffScore = toNum(b[colScore - 1]) - toNum(a[colScore - 1]);
      if (diffScore !== 0) return diffScore;
    }
    if (colPct) {
      var diffPct = toNum(b[colPct - 1]) - toNum(a[colPct - 1]);
      if (diffPct !== 0) return diffPct;
    }
    if (colDays) {
      var diffDays = toNum(a[colDays - 1]) - toNum(b[colDays - 1]);
      if (diffDays !== 0) return diffDays;
    }
    return 0;
  });
}

function findHeaderCol_(headers, name) {
  var key = normalizeKey_(name);
  for (var i = 0; i < headers.length; i++) {
    if (normalizeKey_(headers[i]) === key) {
      return i + 1;
    }
  }
  return 0;
}

function parseScore_(value) {
  var normalized = String(value || "").trim().replace(/[^0-9.-]/g, "");
  var num = Number(normalized);
  return isNaN(num) ? 0 : num;
}

function buildHeaderIndex_(headers) {
  var idx = {};
  for (var i = 0; i < headers.length; i++) {
    var key = normalizeKey_(headers[i]);
    if (key && !idx[key]) {
      idx[key] = i + 1;
    }
  }
  return idx;
}

function resolveColumnSpecs_(specs, sourceIdxByKey) {
  return specs.map(function(spec) {
    var aliases = [spec.header].concat(spec.aliases || []);
    var sourceIndex = null;
    for (var i = 0; i < aliases.length; i++) {
      var key = normalizeKey_(aliases[i]);
      if (sourceIdxByKey[key]) {
        sourceIndex = sourceIdxByKey[key] - 1;
        break;
      }
    }

    return {
      header: spec.header,
      required: spec.required,
      defaultValue: spec.defaultValue == null ? "" : spec.defaultValue,
      sourceIndex: sourceIndex
    };
  });
}

function normalizeKey_(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/\s+/g, "")
    .replace(/[^a-z0-9]/g, "");
}

function normalizeHeaderSpacing_(value) {
  return String(value || "").trim().replace(/\s+/g, " ");
}

function lastNonEmptyCol_(row) {
  var last = 0;
  for (var i = 0; i < row.length; i++) {
    if (String(row[i] || "").trim()) {
      last = i + 1;
    }
  }
  return last;
}

function ensureColumns_(sheet, neededCols) {
  var current = sheet.getMaxColumns();
  if (current < neededCols) {
    sheet.insertColumnsAfter(current, neededCols - current);
  }
}

function ensureNormalSheet_(ss, name) {
  var sheet = ss.getSheetByName(name);
  if (!sheet) {
    return ss.insertSheet(name);
  }

  if (
    typeof sheet.getSheetType === "function" &&
    sheet.getSheetType() === SpreadsheetApp.SheetType.DATASOURCE
  ) {
    return ss.insertSheet(name + "_" + new Date().getTime());
  }

  return sheet;
}

function setWidthByHeader_(sheet, idx, header, width) {
  var col = idx[normalizeKey_(header)];
  if (col) {
    sheet.setColumnWidth(col, width);
  }
}

function setNumberFormatIfExists_(sheet, idx, CONFIG, header, format) {
  var col = idx[normalizeKey_(header)];
  if (!col) {
    return;
  }
  sheet.getRange(CONFIG.dataStartRow, col, CONFIG.dataStartRow <= sheet.getLastRow() ? sheet.getLastRow() - CONFIG.dataStartRow + 1 : 1, 1)
    .setNumberFormat(format);
}
