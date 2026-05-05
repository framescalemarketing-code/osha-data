import test from "node:test";
import assert from "node:assert/strict";
import { enrichCompanyLead, setContactResearchAdapter } from "../api/_lib/contact_enrichment.mjs";

test("default adapter derives deterministic website and contactability", () => {
  const enriched = enrichCompanyLead({
    account_name: "Acme Industrial Labs, Inc.",
    lead_source_type: "osha_incident",
  });
  assert.equal(enriched.companyDomain, "acmeindustriallabs.com");
  assert.equal(enriched.website, "https://www.acmeindustriallabs.com");
  assert.ok(enriched.contactabilityScore >= 70);
  assert.equal(enriched.contactResearchStatus, "unresearched");
});

test("adapter can be swapped for future provider integration", () => {
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
  const enriched = enrichCompanyLead({ account_name: "Anything", lead_source_type: "city_license" });
  assert.equal(enriched.companyDomain, "provider.example");
  assert.equal(enriched.contactabilityScore, 99);
});
