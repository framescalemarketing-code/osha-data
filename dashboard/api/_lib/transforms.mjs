// Shared data-transformation helpers for Vercel API functions.
// Logic extracted from server/index.mjs — keep in sync if the server version changes.

export function normalizeCodes(rawStandards) {
  if (!rawStandards) return [];
  const tokens = String(rawStandards).split("|").map((s) => s.trim()).filter(Boolean);
  const codeRegex = /\b\d{4}\.\d+(?:\([^)]+\))*\b/g;
  const codes = [];
  for (const token of tokens) {
    const matches = token.match(codeRegex);
    if (matches) {
      for (const code of matches) {
        if (!codes.includes(code)) codes.push(code);
      }
    }
  }
  return codes;
}

export function inferIncidentType(row) {
  if (String(row["lead_type"] || "").toLowerCase() === "profile_fit") return "Profile Fit";
  const eyeInjury = Number(row["eye_injury_count"] || 0) > 0;
  const prescription = Number(row["prescription_violation_count"] || 0) > 0;
  const eyeViolation = Number(row["eye_violation_count"] || 0) > 0;
  const openViolation = Number(row["open_eye_violation_count"] || 0) > 0;
  const generalPpe = Number(row["general_ppe_violation_count"] || 0) > 0;
  if (eyeInjury) return "Severe Injury";
  if (prescription) return "Prescription Safety";
  if (openViolation) return "Impact Hazard";
  if (eyeViolation) return "Impact Hazard";
  if (generalPpe) return "General PPE";
  return "General PPE";
}

export function resolveIncidentDate(row) {
  if (row["last_eye_injury_date"]) return { value: row["last_eye_injury_date"], source: "accident" };
  if (row["last_violation_event_date"]) return { value: row["last_violation_event_date"], source: "violation-event" };
  if (row["last_violation_date"]) return { value: row["last_violation_date"], source: "violation-event" };
  if (row["close_case_date"]) return { value: row["close_case_date"], source: "case-close" };
  if (row["open_case_date"]) return { value: row["open_case_date"], source: "case-open" };
  return { value: null, source: "unknown" };
}

export function normalizeNaicsCode(value) {
  const digits = String(value || "").replace(/\D/g, "");
  return digits.length >= 2 ? digits : "";
}

export function industryFromNaics(naicsCode) {
  const code = normalizeNaicsCode(naicsCode);
  if (!code) return "";

  const exact = {
    "211120": "Crude Petroleum Extraction",
    "221122": "Electric Power Distribution",
    "311615": "Poultry Processing",
    "312120": "Breweries",
    "324110": "Petroleum Refineries",
    "325199": "Basic Organic Chemical Manufacturing",
    "325412": "Pharmaceutical Preparation Manufacturing",
    "325413": "In-Vitro Diagnostic Substance Manufacturing",
    "325414": "Biological Product Manufacturing",
    "325510": "Paint and Coating Manufacturing",
    "325611": "Soap and Detergent Manufacturing",
    "325998": "Chemical Product Manufacturing",
    "326199": "Plastic Product Manufacturing",
    "332710": "Machine Shops",
    "332994": "Small Arms Manufacturing",
    "333120": "Construction Machinery Manufacturing",
    "333314": "Optical Instrument and Lens Manufacturing",
    "333415": "HVAC and Commercial Refrigeration Equipment Manufacturing",
    "334413": "Semiconductor and Related Device Manufacturing",
    "334416": "Capacitor, Resistor, Coil, Transformer Manufacturing",
    "334510": "Electromedical and Electrotherapeutic Apparatus Manufacturing",
    "334516": "Analytical Laboratory Instrument Manufacturing",
    "334519": "Measuring and Controlling Device Manufacturing",
    "336411": "Aircraft Manufacturing",
    "336412": "Aircraft Engine and Engine Parts Manufacturing",
    "336413": "Other Aircraft Parts and Equipment Manufacturing",
    "339112": "Surgical and Medical Instrument Manufacturing",
    "339113": "Surgical Appliance and Supplies Manufacturing",
    "423450": "Medical, Dental, and Hospital Equipment Wholesalers",
    "493110": "General Warehousing and Storage",
    "541380": "Testing Laboratories",
    "541714": "Research and Development in Biotechnology",
    "562910": "Remediation Services",
  };

  if (exact[code]) return `${exact[code]} (NAICS ${code})`;

  const prefixRules = [
    ["3254", "Pharmaceutical and Medicine Manufacturing"],
    ["325", "Chemical Manufacturing"],
    ["334", "Computer and Electronic Product Manufacturing"],
    ["3364", "Aerospace Product and Parts Manufacturing"],
    ["336", "Transportation Equipment Manufacturing"],
    ["3391", "Medical Equipment and Supplies Manufacturing"],
    ["339", "Miscellaneous Manufacturing"],
    ["333", "Machinery Manufacturing"],
    ["332", "Fabricated Metal Product Manufacturing"],
    ["311", "Food Manufacturing"],
    ["312", "Beverage and Tobacco Product Manufacturing"],
    ["493", "Warehousing and Storage"],
    ["5417", "Scientific Research and Development Services"],
    ["541", "Professional, Scientific, and Technical Services"],
    ["562", "Waste Management and Remediation Services"],
    ["42", "Merchant Wholesalers"],
    ["23", "Construction"],
    ["31", "Manufacturing"],
    ["32", "Manufacturing"],
    ["33", "Manufacturing"],
  ];

  for (const [prefix, label] of prefixRules) {
    if (code.startsWith(prefix)) return `${label} (NAICS ${code})`;
  }
  return "";
}

export function resolveIndustryLabel(row) {
  const naicsLabel = industryFromNaics(row["naics_code"]);
  if (naicsLabel) return naicsLabel;
  return String(row["industry_segment"] || "").trim() || "Unknown Industry";
}

export function normalizeCaliforniaRegion(row) {
  const state = String(row["site_state"] || "").trim().toUpperCase();
  const city = String(row["site_city"] || "").trim().toUpperCase();
  const rawRegion = String(row["region"] || "").trim().toUpperCase();

  if (state && state !== "CA") return String(row["region"] || "Other").trim() || "Other";

  const geoSrc = String(row["geo_match_source"] || "").toLowerCase();
  if (geoSrc === "bay_radius") return "Northern California";
  if (geoSrc === "san_diego_area") return "Southern California";
  if (geoSrc === "bay_radius|san_diego_area") {
    const bayDist = Number(row["bay_area_distance_miles"] ?? 999999);
    const sdDist = Number(row["san_diego_distance_miles"] ?? 999999);
    return bayDist <= sdDist ? "Northern California" : "Southern California";
  }

  const northernCitiesFallback = new Set([
    "SAN FRANCISCO", "OAKLAND", "BERKELEY", "RICHMOND", "SAN JOSE", "FREMONT",
    "PALO ALTO", "MOUNTAIN VIEW", "SUNNYVALE", "REDWOOD CITY", "SAN MATEO",
    "MENLO PARK", "BURLINGAME", "SOUTH SAN FRANCISCO", "SANTA CLARA",
    "WALNUT CREEK", "CONCORD", "ANTIOCH", "PITTSBURG", "BRENTWOOD",
    "LIVERMORE", "HAYWARD", "SAN LEANDRO", "SAN LORENZO",
  ]);
  const southernCitiesFallback = new Set([
    "SAN DIEGO", "CHULA VISTA", "EL CAJON", "SANTEE", "LA MESA",
    "NATIONAL CITY", "IMPERIAL BEACH", "CORONADO", "POWAY", "ESCONDIDO",
    "VISTA", "OCEANSIDE", "CARLSBAD", "SAN MARCOS", "ENCINITAS", "DEL MAR",
    "LOS ANGELES", "LONG BEACH", "ANAHEIM", "IRVINE", "SANTA ANA",
    "RIVERSIDE", "SAN BERNARDINO",
  ]);

  if (northernCitiesFallback.has(city)) return "Northern California";
  if (southernCitiesFallback.has(city)) return "Southern California";
  if (rawRegion.includes("BAY") || rawRegion.includes("NORTH")) return "Northern California";
  if (rawRegion.includes("LOS ANGELES") || rawRegion.includes("SOUTH") || rawRegion.includes("SAN DIEGO")) return "Southern California";
  return "Northern California";
}

export function toLeadRecord(row) {
  const incidentDateInfo = resolveIncidentDate(row);
  const incidentDateIso = incidentDateInfo.value
    ? String(incidentDateInfo.value).slice(0, 10)
    : "";
  const now = Date.now();
  const lastTouchedDays = incidentDateIso
    ? Math.max(0, Math.floor((now - new Date(`${incidentDateIso}T00:00:00Z`).getTime()) / 86400000))
    : 0;

  const tier = row["lead_tier"] || "P3 Industry Fit";
  const leadType = String(row["lead_type"] || (row["inspection_id"] ? "incident" : "profile_fit"));
  const finalScore = Number(row["final_score"] || 0);
  const isCityLicenseLead = !row["inspection_id"];

  const priorityMap = {
    "P0 Hot Eye": "P0 Ideal",
    "P1 Eye Violation": "P1 Active",
    "P2 PPE Opportunity": "P2 Research",
    "P3 Industry Fit": "P3 Monitor",
  };
  const actionMap = {
    "P0 Hot Eye": "Ideal Call Now",
    "P1 Eye Violation": "Call Now",
    "P2 PPE Opportunity": finalScore >= 30 ? "Call This Week" : "Research Then Call",
    "P3 Industry Fit": "Monitor / Nurture",
  };

  const eyeInjuryDescriptions = String(row["eye_injury_descriptions"] || "")
    .split("|").map((s) => s.trim()).filter(Boolean);
  const emphasisCodes = String(row["emphasis_code_list"] || "")
    .split("|").map((s) => s.trim()).filter(Boolean);

  return {
    id: `lead-${row["inspection_id"] || row["account_name"] || Math.random().toString(16).slice(2)}`,
    company: row["account_name"] || "Unknown Company",
    region: normalizeCaliforniaRegion(row),
    county: row["county"] || "",
    distanceFromMiramarMiles:
      row["san_diego_distance_miles"] != null
        ? Number(row["san_diego_distance_miles"])
        : row["distance_from_miramar_miles"] != null ? Number(row["distance_from_miramar_miles"]) : null,
    bayAreaDistanceMiles:
      row["bay_area_distance_miles"] == null ? null : Number(row["bay_area_distance_miles"]),
    isWithinBayArea50Mi:
      row["is_within_bay_area_50mi"] === true
      || String(row["is_within_bay_area_50mi"] || "").toLowerCase() === "true",
    isSanDiegoArea:
      row["is_san_diego_area"] === true
      || String(row["is_san_diego_area"] || "").toLowerCase() === "true",
    geoMatchSource: String(row["geo_match_source"] || "none"),
    leadType,
    qualifiesIncident3Year:
      row["qualifies_incident_3yr"] === true
      || String(row["qualifies_incident_3yr"] || "").toLowerCase() === "true",
    city: row["site_city"] || "",
    industry: resolveIndustryLabel(row),
    ownerType: row["ownership_type"] || "",

    eyeLeadScore: Number(row["eye_lead_score"] || 0),
    ppeScore: Number(row["ppe_score"] || 0),
    finalScore,
    leadTier: tier,

    overallSalesScore: finalScore,
    eyewearEvidenceScore: Number(row["eye_lead_score"] || 0),
    priority: priorityMap[tier] || "P3 Monitor",
    action: actionMap[tier] || "Monitor / Nurture",

    eyeInjuryCount: Number(row["eye_injury_count"] || 0),
    fatalityCount: Number(row["fatality_count"] || 0),
    faceHeadInjuryCount: Number(row["face_head_injury_count"] || 0),
    eyeInjuryDescriptions,

    eyeViolationCount: Number(row["eye_violation_count"] || 0),
    prescriptionViolationCount: Number(row["prescription_violation_count"] || 0),
    openEyeViolationCount: Number(row["open_eye_violation_count"] || 0),
    generalPpeViolationCount: Number(row["general_ppe_violation_count"] || 0),
    openGeneralPpeViolationCount: Number(row["open_general_ppe_violation_count"] || 0),
    willfulViolationCount: Number(row["willful_violation_count"] || 0),
    repeatViolationCount: Number(row["repeat_violation_count"] || 0),
    totalCurrentPenalty: Number(row["total_current_penalty"] || 0),

    violationEventCount: Number(row["violation_event_count"] || 0),
    contestedViolationCount: Number(row["contested_violation_count"] || 0),
    eyeEmphasisCount: Number(row["eye_emphasis_count"] || 0),
    emphasisCodes,
    relatedInspectionCount: Number(row["related_inspection_count"] || 0),
    formalFollowupCount: Number(row["formal_followup_count"] || 0),
    totalInspectionCount: Number(row["total_inspection_count"] || 0),

    rawViolationCodes: normalizeCodes(row["standards_cited"]),
    openViolations:
      row["has_open_violations"] === true
      || String(row["has_open_violations"] || "").toLowerCase() === "true",

    pitchRecommendation: row["pitch_recommendation"] || "",
    employeeBand: row["employee_band"] || "Unknown",

    incidentDate: incidentDateIso,
    incidentDateSource: incidentDateInfo.source,
    incidentType: inferIncidentType(row),
    lastTouchedDays,
    accountStatus: "New",

    openCaseDate: row["open_case_date"] ? String(row["open_case_date"]).slice(0, 10) : "",
    closeCaseDate: row["close_case_date"] ? String(row["close_case_date"]).slice(0, 10) : "",
    lastEyeInjuryDate: row["last_eye_injury_date"] ? String(row["last_eye_injury_date"]).slice(0, 10) : "",

    needTier: tier === "P0 Hot Eye" || tier === "P1 Eye Violation" ? "Direct Need"
      : tier === "P2 PPE Opportunity" ? "Probable Need" : "Fit Only",
    matchedSources: isCityLicenseLead ? ["City License"] : ["OSHA"],
    reasonToContact: row["pitch_recommendation"] || "",
    whyNow: "",
    recentInspectionContext: "",
    severeIncident: Number(row["eye_injury_count"] || 0) > 0,
  };
}
