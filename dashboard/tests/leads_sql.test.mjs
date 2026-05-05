import test from "node:test";
import assert from "node:assert/strict";
import { buildLeadsSql, DEFAULT_LEADS_LIMIT } from "../shared/leads_sql.mjs";

test("primary query excludes city-license leads by default", () => {
  const sql = buildLeadsSql({ projectId: "p", dataset: "d" });
  assert.match(sql, /AND \(FALSE OR s\.inspection_id IS NOT NULL\)/);
  assert.match(sql, /LIMIT 2000/);
  assert.equal(DEFAULT_LEADS_LIMIT, 2000);
});

test("secondary query allows city-license leads", () => {
  const sql = buildLeadsSql({ projectId: "p", dataset: "d", includeSecondary: true });
  assert.match(sql, /AND \(TRUE OR s\.inspection_id IS NOT NULL\)/);
});

test("suppression rules include strict NAICS and consumer patterns", () => {
  const sql = buildLeadsSql({ projectId: "p", dataset: "d" });
  assert.match(sql, /STARTS_WITH\(REGEXP_REPLACE\(COALESCE\(CAST\(c\.naics_code AS STRING\), ''\), r'\\D', ''\), '44'\)/);
  assert.match(sql, /STARTS_WITH\(REGEXP_REPLACE\(COALESCE\(CAST\(c\.naics_code AS STRING\), ''\), r'\\D', ''\), '45'\)/);
  assert.match(sql, /STARTS_WITH\(REGEXP_REPLACE\(COALESCE\(CAST\(c\.naics_code AS STRING\), ''\), r'\\D', ''\), '72'\)/);
  assert.match(sql, /consumer_name_regex/);
  assert.match(sql, /b2b_exception_regex/);
});

test("balanced bucket selection remains in query", () => {
  const sql = buildLeadsSql({ projectId: "p", dataset: "d", bayTarget: 700, sanDiegoTarget: 700, restTarget: 600 });
  assert.match(sql, /COALESCE\(s\.san_diego_distance_miles, 999999\) <= 50/);
  assert.match(sql, /COALESCE\(s\.bay_area_distance_miles, 999999\) <= 50/);
  assert.match(sql, /geo_bucket = 'san_diego' AND geo_bucket_rank <= 700/);
  assert.match(sql, /geo_bucket = 'bay_area' AND geo_bucket_rank <= 700/);
  assert.match(sql, /geo_bucket = 'rest_ca' AND geo_bucket_rank <= 600/);
  assert.match(sql, /LIMIT GREATEST\(2000 - \(SELECT selected_count FROM bucket_counts\), 0\)/);
});
