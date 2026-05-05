import assert from "node:assert/strict";
import { buildLeadsSql, DEFAULT_LEADS_LIMIT } from "../shared/leads_sql.mjs";
import { enrichCompanyLead, setContactResearchAdapter } from "../api/_lib/contact_enrichment.mjs";

function testLeadsSql() {
  const primary = buildLeadsSql({ projectId: "p", dataset: "d" });
  assert.match(primary, /AND \(FALSE OR s\.inspection_id IS NOT NULL\)/);
  assert.match(primary, /LIMIT 2000/);
  assert.equal(DEFAULT_LEADS_LIMIT, 2000);

  const secondary = buildLeadsSql({ projectId: "p", dataset: "d", includeSecondary: true });
  assert.match(secondary, /AND \(TRUE OR s\.inspection_id IS NOT NULL\)/);

  assert.match(primary, /consumer_name_regex/);
  assert.match(primary, /b2b_exception_regex/);
  assert.match(primary, /'44'/);
  assert.match(primary, /'45'/);
  assert.match(primary, /'72'/);

  const balanced = buildLeadsSql({
    projectId: "p",
    dataset: "d",
    bayTarget: 700,
    sanDiegoTarget: 700,
    restTarget: 600,
  });
  assert.match(balanced, /geo_bucket = 'san_diego' AND geo_bucket_rank <= 700/);
  assert.match(balanced, /geo_bucket = 'bay_area' AND geo_bucket_rank <= 700/);
  assert.match(balanced, /geo_bucket = 'rest_ca' AND geo_bucket_rank <= 600/);
}

function testContactAdapter() {
  const enriched = enrichCompanyLead({
    account_name: "Acme Industrial Labs, Inc.",
    lead_source_type: "osha_incident",
  });
  assert.equal(enriched.companyDomain, "acmeindustriallabs.com");
  assert.equal(enriched.website, "https://www.acmeindustriallabs.com");
  assert.ok(enriched.contactabilityScore >= 70);
  assert.equal(enriched.contactResearchStatus, "unresearched");

  setContactResearchAdapter({
    enrichCompanyLead() {
      return {
        companyDomain: "provider.example",
        website: "https://provider.example",
        contactabilityScore: 99,
        contactResearchStatus: "enriched",
        contactResearchNotes: "provider mock",
      };
    },
  });
  const swapped = enrichCompanyLead({ account_name: "Anything", lead_source_type: "city_license" });
  assert.equal(swapped.companyDomain, "provider.example");
  assert.equal(swapped.contactabilityScore, 99);
}

function main() {
  testLeadsSql();
  testContactAdapter();
  console.log("All dashboard unit checks passed.");
}

main();
