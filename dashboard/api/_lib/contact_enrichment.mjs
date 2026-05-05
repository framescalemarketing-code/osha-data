let adapter = {
  enrichCompanyLead(row) {
    const rawCompany = String(row["account_name"] || "").trim();
    const providedDomain = String(row["company_domain"] || "").trim().toLowerCase();
    const providedWebsite = String(row["website"] || "").trim().toLowerCase();
    const leadSourceType = String(row["lead_source_type"] || "");

    const derivedDomain = providedDomain || guessDomain(rawCompany);
    const website = providedWebsite || (derivedDomain ? `https://www.${derivedDomain}` : "");
    const existingScore = Number(row["contactability_score"] || 0);
    const inferredScore = existingScore > 0 ? existingScore : inferContactability(leadSourceType, website, rawCompany);
    const status = String(row["contact_research_status"] || "").trim() || "unresearched";
    const notes = String(row["contact_research_notes"] || "").trim();

    return {
      companyDomain: derivedDomain || "",
      website,
      contactabilityScore: inferredScore,
      contactResearchStatus: status,
      contactResearchNotes: notes,
    };
  },
};

function guessDomain(companyName) {
  if (!companyName) return "";
  const normalized = companyName
    .toLowerCase()
    .replace(/[^a-z0-9 ]+/g, " ")
    .replace(/\b(inc|llc|ltd|corp|corporation|company|co|plc|lp|llp)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  if (!normalized) return "";
  const joined = normalized.replace(/\s+/g, "");
  if (joined.length < 4) return "";
  return `${joined}.com`;
}

function inferContactability(leadSourceType, website, companyName) {
  let score = 0;
  if (website) score += 50;
  if (companyName && companyName.length >= 8) score += 20;
  if (leadSourceType === "osha_incident") score += 20;
  else if (leadSourceType === "osha_profile") score += 15;
  else if (leadSourceType === "city_license") score += 5;
  return Math.min(100, score);
}

export function setContactResearchAdapter(nextAdapter) {
  if (!nextAdapter || typeof nextAdapter.enrichCompanyLead !== "function") {
    throw new Error("Adapter must implement enrichCompanyLead(row).");
  }
  adapter = nextAdapter;
}

export function enrichCompanyLead(row) {
  return adapter.enrichCompanyLead(row);
}
