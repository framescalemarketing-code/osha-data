function formatActiveSalesFollowupSheet() {
  var sheet = SpreadsheetApp.getActiveSheet();
  applySalesFollowupFormatting_(sheet);
}

function formatNamedSalesFollowupSheet(sheetName) {
  var sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(sheetName);
  if (!sheet) {
    throw new Error('Sheet not found: ' + sheetName);
  }
  applySalesFollowupFormatting_(sheet);
}

function applySalesFollowupFormatting_(sheet) {
  var lastRow = sheet.getLastRow();
  var lastColumn = sheet.getLastColumn();
  if (lastRow < 2 || lastColumn < 2) {
    throw new Error('Sheet does not contain a header row plus data rows.');
  }

  var headerRange = sheet.getRange(1, 1, 1, lastColumn);
  var headers = headerRange.getValues()[0];
  var headerIndex = {};
  headers.forEach(function(header, idx) {
    headerIndex[String(header).trim()] = idx + 1;
  });

  sheet.setFrozenRows(1);
  sheet.setFrozenColumns(2);
  sheet.getDataRange().setVerticalAlignment('middle');
  headerRange
    .setFontWeight('bold')
    .setBackground('#0F172A')
    .setFontColor('#FFFFFF')
    .setWrap(true);

  if (!sheet.getFilter()) {
    sheet.getDataRange().createFilter();
  }

  if (headerIndex['Follow-up Score']) {
    sheet
      .getRange(2, 1, lastRow - 1, lastColumn)
      .sort([{ column: headerIndex['Follow-up Score'], ascending: false }]);
  }

  setNumberFormatIfPresent_(sheet, headerIndex, 'Penalties Total USD', '$#,##0.00');
  setNumberFormatIfPresent_(sheet, headerIndex, 'Follow-up Percentile', '0.0');

  setWrapIfPresent_(sheet, headerIndex, 'Citation Sales Explanation');
  setWrapIfPresent_(sheet, headerIndex, 'Citation Excerpt');
  setWrapIfPresent_(sheet, headerIndex, 'Standards Cited');
  setWrapIfPresent_(sheet, headerIndex, 'Violation Items');

  var rules = [];
  rules = rules.concat(priorityRules_(sheet, headerIndex, lastRow));
  rules = rules.concat(programRules_(sheet, headerIndex, lastRow));
  rules = rules.concat(signalRules_(sheet, headerIndex, lastRow));
  sheet.setConditionalFormatRules(rules);

  sheet.autoResizeColumns(1, Math.min(lastColumn, 12));
  setColumnWidthIfPresent_(sheet, headerIndex, 'Citation Sales Explanation', 360);
  setColumnWidthIfPresent_(sheet, headerIndex, 'Citation Excerpt', 320);
  setColumnWidthIfPresent_(sheet, headerIndex, 'Standards Cited', 220);
  setColumnWidthIfPresent_(sheet, headerIndex, 'Violation Items', 180);
}

function priorityRules_(sheet, headerIndex, lastRow) {
  var column = headerIndex['Follow-up Priority'];
  if (!column) {
    return [];
  }

  var range = sheet.getRange(2, column, Math.max(lastRow - 1, 1), 1);
  return [
    SpreadsheetApp.newConditionalFormatRule()
      .whenTextEqualTo('Priority 1')
      .setBackground('#FEE2E2')
      .setFontColor('#991B1B')
      .setRanges([range])
      .build(),
    SpreadsheetApp.newConditionalFormatRule()
      .whenTextEqualTo('Priority 2')
      .setBackground('#FEF3C7')
      .setFontColor('#92400E')
      .setRanges([range])
      .build(),
    SpreadsheetApp.newConditionalFormatRule()
      .whenTextEqualTo('Priority 3')
      .setBackground('#DCFCE7')
      .setFontColor('#166534')
      .setRanges([range])
      .build()
  ];
}

function programRules_(sheet, headerIndex, lastRow) {
  var column = headerIndex['Program Relevance'];
  if (!column) {
    return [];
  }

  var range = sheet.getRange(2, column, Math.max(lastRow - 1, 1), 1);
  return [
    SpreadsheetApp.newConditionalFormatRule()
      .whenTextEqualTo('Prescription Safety')
      .setBackground('#DBEAFE')
      .setFontColor('#1D4ED8')
      .setRanges([range])
      .build(),
    SpreadsheetApp.newConditionalFormatRule()
      .whenTextEqualTo('General PPE / Eyewear')
      .setBackground('#E0F2FE')
      .setFontColor('#075985')
      .setRanges([range])
      .build()
  ];
}

function signalRules_(sheet, headerIndex, lastRow) {
  var rules = [];
  addTextFlagRule_(rules, sheet, headerIndex, lastRow, 'Severe Incident Signal', 'Yes', '#FEE2E2', '#991B1B');
  addTextFlagRule_(rules, sheet, headerIndex, lastRow, 'Has Open Violations', 'Yes', '#FFF7ED', '#9A3412');
  addTextFlagRule_(rules, sheet, headerIndex, lastRow, 'Has Complaint Signal', 'Yes', '#FEF3C7', '#92400E');
  return rules;
}

function addTextFlagRule_(rules, sheet, headerIndex, lastRow, header, value, bg, fg) {
  var column = headerIndex[header];
  if (!column) {
    return;
  }

  rules.push(
    SpreadsheetApp.newConditionalFormatRule()
      .whenTextEqualTo(value)
      .setBackground(bg)
      .setFontColor(fg)
      .setRanges([sheet.getRange(2, column, Math.max(lastRow - 1, 1), 1)])
      .build()
  );
}

function setNumberFormatIfPresent_(sheet, headerIndex, header, format) {
  var column = headerIndex[header];
  if (!column || sheet.getLastRow() < 2) {
    return;
  }

  sheet.getRange(2, column, sheet.getLastRow() - 1, 1).setNumberFormat(format);
}

function setWrapIfPresent_(sheet, headerIndex, header) {
  var column = headerIndex[header];
  if (!column || sheet.getLastRow() < 2) {
    return;
  }

  sheet.getRange(2, column, sheet.getLastRow() - 1, 1).setWrap(true);
}

function setColumnWidthIfPresent_(sheet, headerIndex, header, width) {
  var column = headerIndex[header];
  if (!column) {
    return;
  }

  sheet.setColumnWidth(column, width);
}
